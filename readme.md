# ASBA-NLP — Final Report Code Submission

Lightweight README for the project code and assets included in this workspace.

## Overview
This repository contains the code, data and notebooks used for Aspect-Based Sentiment Analysis (ABSA) experiments and model training/serving.

## Main files & notebooks
- Project notebook: [NLP_Final_Report_Code_Submission.ipynb](NLP_Final_Report_Code_Submission.ipynb) — core pipeline, training loops and utilities. Key notebook symbols:
  - Model class: [`MultiHeadBert`](NLP_Final_Report_Code_Submission.ipynb)
  - Dataset wrapper: [`ABSADataset`](NLP_Final_Report_Code_Submission.ipynb)
  - Label parsing: [`parse_scores`](NLP_Final_Report_Code_Submission.ipynb) and [`encode_labels_list`](NLP_Final_Report_Code_Submission.ipynb)
  - Evaluation / inference: [`evaluate`](NLP_Final_Report_Code_Submission.ipynb) and [`predict_new`](NLP_Final_Report_Code_Submission.ipynb)

## Scripts & helpers
- [AspectStatistics/overview_of_dataset.py](AspectStatistics/overview_of_dataset.py)
- [DataProcessing/subsets.py](DataProcessing/subsets.py)
- [Mics/categorize.py](Mics/categorize.py)
- [RAG_2_ASBA/absa_labelling.py](RAG_2_ASBA/absa_labelling.py)
- [RAG_2_ASBA/rag.ipynb](RAG_2_ASBA/rag.ipynb)
- [Scrappers/hepsiburada_all.py](Scrappers/hepsiburada_all.py)

## Data & exports
- Raw / exported datasets: [Exports/products.csv](Exports/products.csv), [Exports/reviews.csv](Exports/reviews.csv)
- Document templates and paper files: [Documents/](Documents/)

## Quick start
1. Open and run [NLP_Final_Report_Code_Submission.ipynb](NLP_Final_Report_Code_Submission.ipynb) in a Jupyter/Colab environment.
2. Install dependencies as shown in the notebook cells (e.g., transformers, torch, sentence-transformers, scikit-learn).
3. Use the notebook sections to:
   - prepare and parse labels (`parse_scores`, `encode_labels_list`)
   - build datasets via `ABSADataset`
   - train `MultiHeadBert` (training loops in notebook)
   - evaluate with `evaluate` and run live tests with `predict_new`

## Models & outputs
- Trained checkpoints produced by the notebook (examples):
  - `absa_model_7_aspects.pt`
  - `absa_model_14_aspects.pt`
  - `absa_model_cleaned.pt`
  - `absa_model_light_frozen.pt`
  - `absa_model_classic_fixed.pt`
  - `best_absa_model.pt`

Load these checkpoints from the notebook inference sections.

## Notes
- The notebook contains multiple experimental setups (7 vs 14 aspects, class-weighting, layer freezing, synthetic data augmentation). See [NLP_Final_Report_Code_Submission.ipynb](NLP_Final_Report_Code_Submission.ipynb) for cell-by-cell explanations and commands.
- Use the helper scripts in `Mics/`, `AspectStatistics/` and `DataProcessing/` for dataset inspection and subset creation.

## Contact / reference
Open the notebook and helper scripts linked above to inspect and run the pipeline. For paper files and templates, see [Documents/](Documents/).
