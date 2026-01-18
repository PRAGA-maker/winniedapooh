from pathlib import Path

from datasets import load_dataset


def main() -> None:
    output_dir = Path("temporary/hf_dataset")
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "data.parquet"

    print("Downloading Hugging Face dataset: carpetxie/winniethepooh (train split)")
    hf_dataset = load_dataset("carpetxie/winniethepooh", split="train")
    df = hf_dataset.to_pandas()

    print(f"Rows: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")

    df.to_parquet(parquet_path, index=False)
    print(f"Saved parquet to: {parquet_path}")


if __name__ == "__main__":
    main()
