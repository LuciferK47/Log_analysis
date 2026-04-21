#!/usr/bin/env python3
import os
import glob
import time
import polars as pl
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, classification_report
from imblearn.under_sampling import RandomUnderSampler
import random

def process_lazy_graph(parquet_dir, mission_id):
    att_path = os.path.join(parquet_dir, "ATT.parquet")
    rcou_path = os.path.join(parquet_dir, "RCOU.parquet")

    if not os.path.exists(att_path) or not os.path.exists(rcou_path):
        return None

    att_lf = pl.scan_parquet(att_path).select(["TimeUS", "Roll"])
    rcou_lf = pl.scan_parquet(rcou_path).select(["TimeUS", "C1"])

    att_lf = att_lf.with_columns([
        pl.from_epoch(pl.col("TimeUS"), time_unit="us").alias("timestamp")
    ]).sort("timestamp")
    
    rcou_lf = rcou_lf.with_columns([
        pl.from_epoch(pl.col("TimeUS"), time_unit="us").alias("timestamp")
    ]).sort("timestamp")

    joined_lf = att_lf.join_asof(rcou_lf, on="timestamp", strategy="backward")

    micro_window_lf = joined_lf.rolling(index_column="timestamp", period="2s").agg([
        pl.col("Roll").var().alias("roll_var_2s"),
        pl.col("C1").max().alias("rcou_c1_max_2s")
    ])

    macro_window_lf = joined_lf.rolling(index_column="timestamp", period="30s").agg([
        pl.col("Roll").mean().alias("roll_mean_30s"),
        pl.col("C1").mean().alias("rcou_c1_mean_30s")
    ])

    final_lf = (
        joined_lf
        .join(micro_window_lf, on="timestamp")
        .join(macro_window_lf, on="timestamp")
    ).drop_nulls()
    
    final_lf = final_lf.with_columns(pl.lit(mission_id).alias("mission_id"))
    
    return final_lf

def main():
    print("--- AUTO-LABELED BATCH TRAINING PIPELINE ---")
    dfs = []
    
    # Process all logs exactly the same way to find dynamic onset
    all_dirs = glob.glob("Logs/Healthy/*_parquet") + glob.glob("Logs/Faulty/*_parquet")
    print(f"Processing {len(all_dirs)} total logs...")
    
    for p_dir in all_dirs:
        mission_id = os.path.basename(p_dir)
        lf = process_lazy_graph(p_dir, mission_id)
        if lf is None:
            continue
            
        onset_lf = lf.filter((pl.col("rcou_c1_max_2s") >= 1900) & (pl.col("roll_var_2s") > 0.10)).select("TimeUS")
        onset_df = onset_lf.collect()
        
        if len(onset_df) > 0:
            onset_timeus = onset_df.row(0)[0]
            print(f"  Found Dynamic Onset at {onset_timeus} for {mission_id}")
            df = lf.collect().to_pandas()
            df = df[df['TimeUS'] <= (onset_timeus + 3000000)].copy()
            df['label'] = (df['TimeUS'] >= onset_timeus).astype(int)
            dfs.append(df)
            print(f"  Processed Faulty: {mission_id} -> {len(df)} rows")
        else:
            df = lf.collect().to_pandas()
            df['label'] = 0
            dfs.append(df)
            print(f"  Processed Healthy: {mission_id} -> {len(df)} rows")

    if not dfs:
        print("No valid data found to train model!")
        return

    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal Aggregate Dataset: {len(combined_df)} rows")
    print("Class Distribution Before Under-Sampling:")
    print(combined_df['label'].value_counts().to_string())
    
    print("\nApplying Manual Stratified Group Split...")
    mission_labels = combined_df.groupby('mission_id')['label'].max()
    faulty_mission_ids = mission_labels[mission_labels == 1].index.tolist()
    healthy_mission_ids = mission_labels[mission_labels == 0].index.tolist()
    
    random.seed(42)
    random.shuffle(faulty_mission_ids)
    faulty_test_size = max(1, int(len(faulty_mission_ids) * 0.2))
    test_faulty_ids = faulty_mission_ids[:faulty_test_size]
    train_faulty_ids = faulty_mission_ids[faulty_test_size:]
    
    random.shuffle(healthy_mission_ids)
    healthy_test_size = max(1, int(len(healthy_mission_ids) * 0.2))
    test_healthy_ids = healthy_mission_ids[:healthy_test_size]
    train_healthy_ids = healthy_mission_ids[healthy_test_size:]
    
    train_ids = set(train_faulty_ids + train_healthy_ids)
    test_ids = set(test_faulty_ids + test_healthy_ids)
    
    train_df = combined_df[combined_df['mission_id'].isin(train_ids)].copy()
    test_df = combined_df[combined_df['mission_id'].isin(test_ids)].copy()
    
    print("\nTrain Mission IDs:")
    print(train_df['mission_id'].unique())
    print("\nTest Mission IDs:")
    print(test_df['mission_id'].unique())
    
    features = ['roll_var_2s', 'rcou_c1_max_2s', 'roll_mean_30s', 'rcou_c1_mean_30s']
    X_train = train_df[features]
    y_train = train_df['label']
    X_test = test_df[features]
    y_test = test_df['label']

    print("\nApplying Random Under-Sampling to TRAINING set ONLY (sampling_strategy=0.1)...")
    rus = RandomUnderSampler(sampling_strategy=0.1, random_state=42)
    X_train_resampled, y_train_resampled = rus.fit_resample(X_train, y_train)
    
    print("Training Class Distribution After Under-Sampling:")
    print(y_train_resampled.value_counts().to_string())

    class_counts = y_train_resampled.value_counts()
    scale_weight = class_counts[0] / class_counts[1]
    
    print(f"\nTraining XGBoost (scale_pos_weight={scale_weight:.2f}) with Strong Regularization...")
    model = xgb.XGBClassifier(
        n_estimators=100, max_depth=3, learning_rate=0.1,
        colsample_bytree=0.5, subsample=0.8, scale_pos_weight=scale_weight,
        use_label_encoder=False, eval_metric='logloss', random_state=42
    )

    t0 = time.time()
    model.fit(X_train_resampled, y_train_resampled)
    print(f"Training completed in {(time.time() - t0):.4f} seconds.")

    print("\n--- Evaluation on Test Set ---")
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Test Accuracy: {accuracy * 100:.2f}%\n")
    print("Classification Report:")
    print(classification_report(y_test, y_pred))

    print("\nFeature Importances:")
    for feature, imp in zip(features, model.feature_importances_):
        print(f"  {feature}: {imp:.4f}")

    export_path = "xgboost_fallback.json"
    model.save_model(export_path)
    print(f"\nModel strictly exported to: {export_path}")

if __name__ == "__main__":
    main()
