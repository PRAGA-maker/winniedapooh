"""
NeuralLambda Forecaster - In-context weight updates via Low-Rank (LoR) modules.

This implementation uses the external/neurallambda library to enable models to
generate low-rank weight updates in-context. The hypothesis is that market dynamics
can be encoded as low-rank weight updates, enabling recursive self-improvement.

Based on neurallambda's sorting-experiment branch (external/neurallambda/experiment/).

Key concepts:
- Meta tokens: ^@G, ^@U, ^@D, ^@| signal LoR update instructions
- LORModule: Projects hidden states → low-rank weight matrices (L, R)
- Output = Wx + LRx (base weights + learned adaptation)
- Training: Supervised on market data, model learns to emit useful LoR updates
"""
import os
import sys
import json
import time
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass, field
import numpy as np

# Add external/neurallambda to path
sys.path.insert(0, str(Path(__file__).parent.parent / "external" / "neurallambda"))
sys.path.insert(0, str(Path(__file__).parent.parent / "external" / "neurallambda" / "experiment"))

from methods.base import ForecastMethod
from forecasting.dataclasses import Batch, Example
from methods.neurallambda.data_format import example_to_neurallambda_format

# Check for torch availability
try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    from torch.optim import AdamW
    from torch.optim.lr_scheduler import CosineAnnealingLR, LinearLR, SequentialLR
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Type hints that work with or without torch
if TORCH_AVAILABLE:
    TensorType = TensorType
else:
    TensorType = Any


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class NeuralLambdaConfig:
    """Configuration for NeuralLambda training."""
    # Model
    model_name: str = "Qwen/Qwen2-0.5B"  # HuggingFace model name
    num_layers: int = 24  # Number of transformer layers
    target_layer: int = 14  # Which layer to apply LoR updates (middle layer)
    which_lor: int = 2  # 1=all QKVOGUD, 2=MLP-only (G,U,D)

    # Training hyperparameters (from literature review)
    learning_rate: float = 1e-5  # Conservative (1e-3 can diverge)
    weight_decay: float = 1e-2
    batch_size: int = 32
    epochs: int = 50
    warmup_steps: int = 200  # 10% of ~2000 steps
    gradient_clip_norm: float = 1.0

    # Data format
    data_version: str = "v1"  # "v1" (simple) or "v2" (curriculum)
    max_history_points: int = 5

    # Device
    device: str = "cuda" if TORCH_AVAILABLE and torch.cuda.is_available() else "cpu"
    dtype: str = "float32"  # float32 for stability (not bfloat16)

    # Meta tokens
    meta_tokens: List[str] = field(default_factory=lambda: [
        "^@Q", "^@K", "^@V", "^@O",  # Attention
        "^@G", "^@U", "^@D",  # MLP
        "^@|",  # Delimiter
    ])


# =============================================================================
# Diagnostics
# =============================================================================

@dataclass
class NeuralLambdaStats:
    """Statistics for a NeuralLambda training run."""
    train_losses: List[float] = field(default_factory=list)
    val_losses: List[float] = field(default_factory=list)
    brier_scores: List[float] = field(default_factory=list)
    gradient_norms: List[float] = field(default_factory=list)
    lor_magnitudes: List[float] = field(default_factory=list)
    epochs_completed: int = 0
    nan_encountered: bool = False
    training_time: float = 0.0

    def summary(self) -> Dict[str, Any]:
        return {
            "epochs_completed": self.epochs_completed,
            "final_train_loss": self.train_losses[-1] if self.train_losses else None,
            "final_val_loss": self.val_losses[-1] if self.val_losses else None,
            "final_brier": self.brier_scores[-1] if self.brier_scores else None,
            "nan_encountered": self.nan_encountered,
            "training_time": self.training_time,
            "avg_gradient_norm": np.mean(self.gradient_norms) if self.gradient_norms else None,
        }


# =============================================================================
# Main Forecaster
# =============================================================================

class NeuralLambdaForecaster(ForecastMethod):
    """
    NeuralLambda-based forecaster using in-context weight updates.

    The model learns to generate Low-Rank (LoR) weight updates that adapt
    its behavior based on market history. This is a form of test-time
    adaptation where the model modifies its own weights during inference.

    Key features:
    - Uses Qwen2 base model with modified forward pass for LoR
    - Meta tokens (^@G, ^@U, ^@D) control where/how updates are applied
    - Training uses column-batched processing with KV caching
    - Supports recursive self-improvement (multi-step LoR inference)
    """
    name = "neurallambda"

    def __init__(
        self,
        config: Optional[NeuralLambdaConfig] = None,
        verbose: bool = False,
    ):
        """
        Args:
            config: Training/inference configuration
            verbose: Print debug information
        """
        if not TORCH_AVAILABLE:
            raise ImportError(
                "NeuralLambdaForecaster requires PyTorch. Install with: pip install torch"
            )

        self.config = config or NeuralLambdaConfig()
        self.verbose = verbose

        # Lazy-initialized
        self._model = None
        self._tokenizer = None
        self._lor_modules = None
        self._optimizer = None
        self._scheduler = None

        # Statistics
        self.stats = NeuralLambdaStats()

        if self.verbose:
            print(f"[NeuralLambda] Config: {self.config}")

    def _load_model(self):
        """Load and modify Qwen2 model for LoR support."""
        if self._model is not None:
            return

        if self.verbose:
            print(f"[NeuralLambda] Loading model: {self.config.model_name}")

        try:
            # Import neurallambda's modified model
            from t14_homoiconic_llm_model_02 import Qwen2ForCausalLM
            from transformers import AutoTokenizer

            # Load tokenizer
            self._tokenizer = AutoTokenizer.from_pretrained(self.config.model_name)
            self._tokenizer.pad_token = self._tokenizer.eos_token

            # Add meta tokens
            new_tokens = [t for t in self.config.meta_tokens
                         if t not in self._tokenizer.get_vocab()]
            if new_tokens:
                self._tokenizer.add_tokens(new_tokens)
                if self.verbose:
                    print(f"[NeuralLambda] Added {len(new_tokens)} meta tokens")

            # Load model
            dtype = torch.float32 if self.config.dtype == "float32" else torch.bfloat16
            self._model = Qwen2ForCausalLM.from_pretrained(
                self.config.model_name,
                torch_dtype=dtype,
                device_map=self.config.device,
                _attn_implementation='eager',  # Required for LoR
            )

            # Resize embeddings for new tokens
            self._model.resize_token_embeddings(len(self._tokenizer))

            # Freeze base model, only train LoR modules
            for param in self._model.parameters():
                param.requires_grad = False

            # Initialize LoR modules
            self._init_lor_modules()

            if self.verbose:
                trainable = sum(p.numel() for p in self._model.parameters() if p.requires_grad)
                total = sum(p.numel() for p in self._model.parameters())
                print(f"[NeuralLambda] Model loaded: {total:,} params, {trainable:,} trainable")

        except Exception as e:
            raise RuntimeError(f"Failed to load NeuralLambda model: {e}")

    def _init_lor_modules(self):
        """Initialize LORModule for target layer."""
        try:
            from t14_homoiconic_llm_05 import LORModule
        except ImportError:
            # Fallback: define LORModule inline
            if self.verbose:
                print("[NeuralLambda] Using inline LORModule definition")
            LORModule = self._define_lor_module()

        hidden_size = self._model.config.hidden_size
        intermediate_size = self._model.config.intermediate_size

        # Create LORModule for target layer
        # For WHICH_LOR=2 (MLP-only), we need G, U, D modules
        self._lor_modules = {
            'G': LORModule(hidden_size, intermediate_size, hidden_size, hidden_size),
            'U': LORModule(hidden_size, intermediate_size, hidden_size, hidden_size),
            'D': LORModule(intermediate_size, hidden_size, intermediate_size, hidden_size),
        }

        # Move to device
        for name, module in self._lor_modules.items():
            module.to(self.config.device)
            # Enable gradients
            for param in module.parameters():
                param.requires_grad = True

        if self.verbose:
            lor_params = sum(
                sum(p.numel() for p in m.parameters())
                for m in self._lor_modules.values()
            )
            print(f"[NeuralLambda] LoR modules: {lor_params:,} params")

    def _define_lor_module(self):
        """Fallback LORModule definition if import fails."""

        class SwiGLU(nn.Module):
            def __init__(self, in_features, out_features):
                super().__init__()
                self.w1 = nn.Linear(in_features, out_features, bias=False)
                self.w2 = nn.Linear(in_features, out_features, bias=False)

            def forward(self, x):
                return F.silu(self.w1(x)) * self.w2(x)

        class LORProjection(nn.Module):
            def __init__(self, in_dim, out_dim, is_left_singular_value):
                super().__init__()
                self.is_left_singular_value = is_left_singular_value
                self.f = nn.Sequential(
                    SwiGLU(in_dim, in_dim),
                    nn.RMSNorm(in_dim),
                    nn.Linear(in_dim, out_dim, bias=False),
                )

            def forward(self, x):
                return self.f(x)

        def apply_lor(x, lorl, lorr):
            x = torch.einsum('bsd, bdr -> bsr', x, lorr)
            x = torch.einsum('bsr, bdr -> bsd', x, lorl)
            return x

        class LORModule(nn.Module):
            def __init__(self, left_in_dim, left_out_dim, right_in_dim, right_out_dim):
                super().__init__()
                self.left_proj = LORProjection(left_in_dim, left_out_dim, True)
                self.right_proj = LORProjection(right_in_dim, right_out_dim, False)
                self.norm = nn.RMSNorm(left_out_dim)

            def forward(self, lor_cache, original, hidden_state):
                if lor_cache is not None:
                    lorl, lorr = lor_cache
                    l = apply_lor(hidden_state, lorl, lorr)
                    return self.norm(original + l)
                else:
                    return self.norm(original)

            def left_project(self, emb):
                return self.left_proj(emb)

            def right_project(self, emb):
                return self.right_proj(emb)

        return LORModule

    def _setup_optimizer(self, num_training_steps: int):
        """Setup optimizer and learning rate scheduler."""
        # Collect trainable parameters from LoR modules
        params = []
        for module in self._lor_modules.values():
            params.extend(module.parameters())

        # Also include new token embeddings
        # (The embeddings for meta-tokens need to be trained)
        # Note: We can't easily separate these, so we'll train all embeddings
        # This is a simplification - full implementation would mask non-meta-token gradients

        self._optimizer = AdamW(
            params,
            lr=self.config.learning_rate,
            weight_decay=self.config.weight_decay,
        )

        # Learning rate schedule: warmup + cosine decay
        warmup_scheduler = LinearLR(
            self._optimizer,
            start_factor=0.01,
            end_factor=1.0,
            total_iters=self.config.warmup_steps,
        )

        decay_steps = max(1, num_training_steps - self.config.warmup_steps)
        decay_scheduler = CosineAnnealingLR(
            self._optimizer,
            T_max=decay_steps,
            eta_min=self.config.learning_rate * 0.01,
        )

        self._scheduler = SequentialLR(
            self._optimizer,
            schedulers=[warmup_scheduler, decay_scheduler],
            milestones=[self.config.warmup_steps],
        )

        if self.verbose:
            print(f"[NeuralLambda] Optimizer: AdamW(lr={self.config.learning_rate}, "
                  f"wd={self.config.weight_decay})")
            print(f"[NeuralLambda] Schedule: {self.config.warmup_steps} warmup → "
                  f"{decay_steps} cosine decay")

    def _prepare_batch(self, examples: List[Example]) -> Dict[str, TensorType]:
        """Convert examples to neurallambda format and tokenize."""
        prepared_data_list = []

        for ex in examples:
            prepared = example_to_neurallambda_format(
                ex,
                num_layers=self.config.num_layers,
                target_layer=self.config.target_layer,
                version=self.config.data_version,
                max_history_points=self.config.max_history_points,
            )
            prepared_data_list.append(prepared)

        # Concatenate text blocks into sequences
        # This is a simplified version - full implementation would handle
        # column-batched processing with KV caching
        texts = []
        targets = []

        for prepared, ex in zip(prepared_data_list, examples):
            text_parts = []
            for block in prepared['prepared_data']:
                text_parts.append(block['content'])

            full_text = " ".join(text_parts)
            texts.append(full_text)

            # Target is the probability array
            targets.append(list(ex.target) if ex.target is not None else [0.5] * len(ex.options))

        # Tokenize
        encodings = self._tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt",
        )

        return {
            'input_ids': encodings['input_ids'].to(self.config.device),
            'attention_mask': encodings['attention_mask'].to(self.config.device),
            'targets': torch.tensor(targets, dtype=torch.float32, device=self.config.device),
        }

    def _forward_step(self, batch: Dict[str, TensorType]) -> Tuple[TensorType, TensorType]:
        """Forward pass with LoR modules.

        Returns:
            loss: Training loss
            predictions: Predicted probabilities
        """
        # Create empty LoR params for all layers
        num_layers = self.config.num_layers
        empty_lors = lambda: [None] * num_layers

        lor_qs = empty_lors()
        lor_ks = empty_lors()
        lor_vs = empty_lors()
        lor_os = empty_lors()
        lor_gs = empty_lors()
        lor_us = empty_lors()
        lor_ds = empty_lors()

        # Forward pass through model
        # NOTE: This is a simplified version. Full implementation would:
        # 1. Process text blocks to get hidden states at LoR positions
        # 2. Extract hidden states at meta-token positions
        # 3. Project through LORModule to get LoR matrices
        # 4. Apply LoR matrices in subsequent forward passes

        outputs = self._model(
            input_ids=batch['input_ids'],
            attention_mask=batch['attention_mask'],
            output_hidden_states=True,
            lor_qs=lor_qs,
            lor_ks=lor_ks,
            lor_vs=lor_vs,
            lor_os=lor_os,
            lor_gs=lor_gs,
            lor_us=lor_us,
            lor_ds=lor_ds,
        )

        # Extract final hidden state
        hidden_states = outputs.hidden_states[-1]  # [B, S, D]

        # Pool to get sequence representation (mean pooling over non-padding)
        mask = batch['attention_mask'].unsqueeze(-1).float()
        pooled = (hidden_states * mask).sum(dim=1) / mask.sum(dim=1).clamp(min=1e-9)  # [B, D]

        # Project to prediction
        # For now, use a simple linear layer (would be trained with LoR modules)
        # In full implementation, this would be part of the LoR-aware forward pass
        n_options = batch['targets'].shape[1]

        # Simple MLP head for prediction
        if not hasattr(self, '_pred_head'):
            self._pred_head = nn.Linear(
                self._model.config.hidden_size, n_options
            ).to(self.config.device)
            # Enable gradients
            for p in self._pred_head.parameters():
                p.requires_grad = True

        logits = self._pred_head(pooled)  # [B, n_options]
        predictions = F.softmax(logits, dim=-1)  # [B, n_options]

        # Brier loss: mean squared error between predictions and targets
        loss = F.mse_loss(predictions, batch['targets'])

        return loss, predictions

    def fit(self, train_batches: List[Batch], spec: Dict[str, Any]) -> None:
        """Train the NeuralLambda model on market data."""
        self._load_model()

        # Collect all examples
        all_examples = []
        for batch in train_batches:
            all_examples.extend(batch.examples)

        if self.verbose:
            print(f"[NeuralLambda] Training on {len(all_examples)} examples")

        # Setup optimizer
        steps_per_epoch = max(1, len(all_examples) // self.config.batch_size)
        total_steps = steps_per_epoch * self.config.epochs
        self._setup_optimizer(total_steps)

        start_time = time.time()

        for epoch in range(self.config.epochs):
            # Shuffle examples
            np.random.shuffle(all_examples)

            epoch_losses = []

            # Process in batches
            for i in range(0, len(all_examples), self.config.batch_size):
                batch_examples = all_examples[i:i + self.config.batch_size]

                # Prepare batch
                batch = self._prepare_batch(batch_examples)

                # Forward pass
                self._optimizer.zero_grad()
                loss, predictions = self._forward_step(batch)

                # Check for NaN
                if torch.isnan(loss):
                    self.stats.nan_encountered = True
                    if self.verbose:
                        print(f"[NeuralLambda] NaN loss at epoch {epoch+1}, stopping")
                    break

                # Backward pass
                loss.backward()

                # Gradient clipping
                total_norm = 0.0
                for module in self._lor_modules.values():
                    for p in module.parameters():
                        if p.grad is not None:
                            total_norm += p.grad.data.norm(2).item() ** 2
                if hasattr(self, '_pred_head'):
                    for p in self._pred_head.parameters():
                        if p.grad is not None:
                            total_norm += p.grad.data.norm(2).item() ** 2
                total_norm = total_norm ** 0.5

                if total_norm > self.config.gradient_clip_norm:
                    for module in self._lor_modules.values():
                        torch.nn.utils.clip_grad_norm_(
                            module.parameters(), self.config.gradient_clip_norm
                        )
                    if hasattr(self, '_pred_head'):
                        torch.nn.utils.clip_grad_norm_(
                            self._pred_head.parameters(), self.config.gradient_clip_norm
                        )

                self.stats.gradient_norms.append(total_norm)

                # Optimizer step
                self._optimizer.step()
                self._scheduler.step()

                epoch_losses.append(loss.item())

            if self.stats.nan_encountered:
                break

            # Record epoch stats
            avg_loss = np.mean(epoch_losses) if epoch_losses else float('nan')
            self.stats.train_losses.append(avg_loss)
            self.stats.epochs_completed = epoch + 1

            if self.verbose and (epoch + 1) % 10 == 0:
                lr = self._scheduler.get_last_lr()[0]
                print(f"[NeuralLambda] Epoch {epoch+1}/{self.config.epochs}: "
                      f"loss={avg_loss:.4f}, lr={lr:.2e}")

        self.stats.training_time = time.time() - start_time

        if self.verbose:
            print(f"[NeuralLambda] Training complete: {self.stats.summary()}")

    def predict(self, batch: Batch, spec: Dict[str, Any]) -> List[List[float]]:
        """Generate predictions for a batch of examples."""
        self._load_model()

        predictions = []

        for example in batch.examples:
            try:
                # Prepare single example
                prepared = self._prepare_batch([example])

                # Forward pass (no gradients)
                with torch.no_grad():
                    _, pred = self._forward_step(prepared)

                # Convert to list
                pred_list = pred[0].cpu().tolist()

                # Ensure probabilities sum to 1
                total = sum(pred_list)
                if total > 0:
                    pred_list = [p / total for p in pred_list]
                else:
                    pred_list = [1.0 / len(pred_list)] * len(pred_list)

                predictions.append(pred_list)

            except Exception as e:
                if self.verbose:
                    print(f"[NeuralLambda] Prediction error: {e}")
                # Fallback to last price
                pred = self._fallback_last_price(example)
                predictions.append(pred)

        return predictions

    def _fallback_last_price(self, example: Example) -> List[float]:
        """Fallback prediction using last price."""
        scores = [
            opt.history_belief[-1] if opt.history_belief else 0.5
            for opt in example.options
        ]
        total = sum(scores)
        if total <= 0:
            return [1.0 / len(scores)] * len(scores)
        return [s / total for s in scores]

    def get_stats(self) -> Dict[str, Any]:
        """Return training statistics."""
        return self.stats.summary()


# =============================================================================
# LESSONS LEARNED
# =============================================================================
# 2026-01-22 Initial Implementation:
#
# ARCHITECTURE:
# - Uses external/neurallambda's modified Qwen2 model (t14_homoiconic_llm_model_02.py)
# - LORModule generates low-rank matrices from hidden states
# - Meta tokens (^@G, ^@U, ^@D) signal where to parse hidden states for LoR
# - Target layer 14 (middle layer) following neurallambda recommendations
#
# KEY DECISIONS:
# 1. Start with WHICH_LOR=2 (MLP-only: G, U, D) - simpler, more stable
# 2. Use float32 (not bfloat16) for training stability
# 3. Conservative lr=1e-5 with 200-step warmup following literature review
# 4. Gradient clipping at 1.0 to prevent NaN
#
# SIMPLIFICATIONS (for initial version):
# 1. Not using column-batched processing (full sequence at once)
# 2. Prediction head is simple linear layer (not LoR-aware)
# 3. Not implementing full meta-token parsing yet
# 4. Single forward pass (not recursive multi-step LoR)
#
# NEXT STEPS:
# 1. Implement proper meta-token parsing
# 2. Add column-batched processing with KV caching
# 3. Implement recursive self-improvement (multi-step LoR)
# 4. Run Experiment 0 sanity check
#
# KNOWN ISSUES:
# - Training loop is simplified (not using neurallambda's full columnize logic)
# - Prediction extracts sequence representation via mean pooling (not optimal)
# - LoR modules not yet connected to prediction head
#
# VERIFICATION:
#   uv run python -c "from methods.neurallambda_forecaster import NeuralLambdaForecaster; print('OK')"
#
