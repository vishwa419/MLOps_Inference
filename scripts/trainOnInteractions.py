import pandas as pd
import numpy as np
from lightfm import LightFM
from lightfm.evaluation import precision_at_k, recall_at_k, auc_score
import pickle
import time
import mlflow
import mlflow.sklearn
from tqdm import tqdm

# Set MLflow tracking URI
mlflow.set_tracking_uri("file:./mlruns")
mlflow.set_experiment("movie-recommender")

print("Loading preprocessed interactions...")
with open('models/dataset.pkl', 'rb') as f:
    dataset = pickle.load(f)

with open('models/train_interactions.pkl', 'rb') as f:
    train_interactions, train_weights = pickle.load(f)

with open('models/test_interactions.pkl', 'rb') as f:
    test_interactions, test_weights = pickle.load(f)

print(f"Train matrix: {train_interactions.shape}, nnz: {train_interactions.nnz:,}")
print(f"Test matrix: {test_interactions.shape}, nnz: {test_interactions.nnz:,}")

# Hyperparameters
params = {
    'loss': 'warp',
    'no_components': 30,
    'learning_rate': 0.05,
    'epochs': 10,
    'num_threads': 4,
    'random_state': 42
}

# Start MLflow run
with mlflow.start_run(run_name="lightfm_baseline"):
    
    # Log parameters
    mlflow.log_params(params)
    mlflow.log_param("train_interactions", train_interactions.nnz)
    mlflow.log_param("test_interactions", test_interactions.nnz)
    
    print("\nInitializing model...")
    model = LightFM(
        loss=params['loss'],
        no_components=params['no_components'],
        learning_rate=params['learning_rate'],
        random_state=params['random_state']
    )
    
    print("\nTraining model...")
    start_time = time.time()
    
    # Train with progress bar
    for epoch in tqdm(range(params['epochs']), desc="Training epochs"):
        model.fit_partial(
            train_interactions,
            sample_weight=train_weights,
            epochs=1,
            num_threads=params['num_threads'],
            verbose=False
        )
    
    train_time = time.time() - start_time
    mlflow.log_metric("train_time_seconds", train_time)
    print(f"\nTraining completed in {train_time:.2f} seconds ({train_time/60:.2f} minutes)")
    
    # Evaluate
    print("\nEvaluating model (this will take 10-20 minutes)...")
    
    print("Computing train precision@10...")
    train_precision = precision_at_k(model, train_interactions, k=10, num_threads=4).mean()
    print(f"  Train Precision@10: {train_precision:.4f}")
    
    print("Computing train recall@10...")
    train_recall = recall_at_k(model, train_interactions, k=10, num_threads=4).mean()
    print(f"  Train Recall@10: {train_recall:.4f}")
    
    print("Computing train AUC...")
    train_auc = auc_score(model, train_interactions, num_threads=4).mean()
    print(f"  Train AUC: {train_auc:.4f}")
    
    print("\nComputing test precision@10...")
    test_precision = precision_at_k(model, test_interactions, k=10, train_interactions=train_interactions, num_threads=4).mean()
    print(f"  Test Precision@10: {test_precision:.4f}")
    
    print("Computing test recall@10...")
    test_recall = recall_at_k(model, test_interactions, k=10, train_interactions=train_interactions, num_threads=4).mean()
    print(f"  Test Recall@10: {test_recall:.4f}")
    
    print("Computing test AUC...")
    test_auc = auc_score(model, test_interactions, train_interactions=train_interactions, num_threads=4).mean()
    print(f"  Test AUC: {test_auc:.4f}")
    
    # Log metrics
    mlflow.log_metric("train_precision_at_10", train_precision)
    mlflow.log_metric("test_precision_at_10", test_precision)
    mlflow.log_metric("train_recall_at_10", train_recall)
    mlflow.log_metric("test_recall_at_10", test_recall)
    mlflow.log_metric("train_auc", train_auc)
    mlflow.log_metric("test_auc", test_auc)
    
    print("\n=== RESULTS ===")
    print(f"Train Precision@10: {train_precision:.4f}")
    print(f"Test Precision@10:  {test_precision:.4f}")
    print(f"\nTrain Recall@10: {train_recall:.4f}")
    print(f"Test Recall@10:  {test_recall:.4f}")
    print(f"\nTrain AUC: {train_auc:.4f}")
    print(f"Test AUC:  {test_auc:.4f}")
    
    # Save model
    print("\nSaving model...")
    with open('models/lightfm_model.pkl', 'wb') as f:
        pickle.dump(model, f)
    
    # Log artifacts to MLflow
    mlflow.log_artifact('models/lightfm_model.pkl')
    mlflow.log_artifact('models/dataset.pkl')
    
    print(f"\nMLflow run completed!")
    print(f"View results: mlflow ui")
