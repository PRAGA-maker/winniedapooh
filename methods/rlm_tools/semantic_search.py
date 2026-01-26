"""
TF-IDF based semantic search over market titles/descriptions.
Zero API calls - uses scikit-learn's TfidfVectorizer.
"""
from typing import List, Dict, Any, Optional, Tuple
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np


class MarketSearchIndex:
    """TF-IDF based search index for market data."""
    
    def __init__(self, max_features: int = 5000):
        self.vectorizer = TfidfVectorizer(
            stop_words='english',
            max_features=max_features,
            ngram_range=(1, 2)  # unigrams + bigrams for better matching
        )
        self.market_texts: List[str] = []
        self.market_ids: List[str] = []
        self.market_metadata: List[Dict[str, Any]] = []
        self.tfidf_matrix: Optional[np.ndarray] = None
        self.is_built = False
    
    def add_market(self, market_id: str, title: str, description: str = "", 
                   metadata: Optional[Dict[str, Any]] = None) -> None:
        """Add a market to the index (call before build_index)."""
        text = f"{title} {description}".strip()
        self.market_texts.append(text)
        self.market_ids.append(market_id)
        self.market_metadata.append(metadata or {})
    
    def build_index(self) -> None:
        """Build TF-IDF matrix from all added markets."""
        if not self.market_texts:
            return
        self.tfidf_matrix = self.vectorizer.fit_transform(self.market_texts)
        self.is_built = True
    
    def search(self, query: str, top_k: int = 5) -> List[Tuple[str, float, Dict[str, Any]]]:
        """
        Search for markets similar to query.
        
        Returns:
            List of (market_id, similarity_score, metadata) tuples
        """
        if not self.is_built or self.tfidf_matrix is None:
            return []
        
        query_vec = self.vectorizer.transform([query])
        similarities = cosine_similarity(query_vec, self.tfidf_matrix)[0]
        
        # Get top-k indices
        top_indices = similarities.argsort()[-top_k:][::-1]
        
        results = []
        for idx in top_indices:
            if similarities[idx] > 0:  # Only include if there's some similarity
                results.append((
                    self.market_ids[idx],
                    float(similarities[idx]),
                    self.market_metadata[idx]
                ))
        return results
    
    def get_market_text(self, market_id: str) -> Optional[str]:
        """Get the text for a specific market."""
        try:
            idx = self.market_ids.index(market_id)
            return self.market_texts[idx]
        except ValueError:
            return None


# --- LESSONS LEARNED ---
# 1. TF-IDF with bigrams captures phrases like "presidential election" better.
# 2. Build index once, search many times - index building is O(n*features).
# 3. cosine_similarity is efficient for sparse TF-IDF matrices.
