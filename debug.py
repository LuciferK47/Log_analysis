from prototype.ingestion import LogReader
from prototype.abstraction import FeatureExtractor

config_path = 'prototype/feature_registry.yaml'
reader = LogReader('Logs/00000008.BIN')
df = reader.read_and_resample(target_hz=10, config_path=config_path)

extractor = FeatureExtractor(config_path)
features_ts = extractor.compute_features(df)

print("Columns in df:", df.columns.tolist())
print("Columns in features_ts:", features_ts.columns.tolist())
for col in features_ts.columns:
    if col != '__flight_mode__':
        print(f"Max {col}: {features_ts[col].max()}")
