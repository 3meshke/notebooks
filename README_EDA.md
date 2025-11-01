# EDA Validation Notebooks for Customer Foundation Model

## Overview

This directory contains comprehensive Exploratory Data Analysis (EDA) notebooks for validating the Customer Foundation Model using **spark** for data processing.

All notebooks are designed to run on Databricks (runtime 14.3 ML) and use memory-efficient techniques to handle large-scale datasets while preserving critical data structure constraints:
- **Per-customer isolation**: Each customer_id appears in exactly ONE chunk folder
- **Chunk organization**: Chunks 0-255 (train), 256-319 (valid)
- **Time splits**: In-time (pre-2024) vs OOT (2024+)

### Processing Approach (Prioritizing Accuracy):

**Notebooks 01-02** (Customer & Target Validation):
- **True chunked processing**: Process one chunk at a time, never load full dataset
- **Incremental aggregation**: Build statistics (counts, sums) incrementally
- **Best for**: Counting operations, customer tracking

**Notebooks 03-04** (Feature Profiling & Drift):
- **Full table with sampling**: Load complete table (or sub-sampled) for accurate statistics
- **Accurate calculations**: Exact median, percentiles, PSI scores
- **One table at a time**: Process sequentially, free up the memory between tables
- **Best for**: Statistical distributions, drift detection

**All Notebooks**:
- **Selective column loading**: Only load necessary columns with `usecols`
- **Memory management**: Explicit garbage collection after processing
- **ADLS integration**: Direct file operations using `dbutils.fs`
- **Hybrid efficiency**: Leverage Spark for Parquet reading, combination of spark and pandas for analysis

---

## Important: Two Processing Strategies

```
┌─────────────────────────────────────────────────────────────────────────┐
│  ACCURACY PRIORITIZED - Different strategies for different statistics  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Notebooks 01-02: TRUE INCREMENTAL PROCESSING                          │
│  ├─ Statistics: Counts, sums (can aggregate incrementally)            │
│  ├─ Memory: ~500-800 MB constant (all 320 chunks)                     │
│  └─ Works with: Any sampling ratio (even 100%)                        │
│                                                                         │
│  Notebooks 03-04: SAMPLED FULL-TABLE PROCESSING                        │
│  ├─ Statistics: Median, percentiles, PSI (require all values)         │
│  ├─ Memory: Scales with sampling (1%→2GB, 100%→20GB per table)       │
│  ├─ Why: Cannot calculate accurate median/percentiles incrementally   │
│  └─ Mitigation: One table at a time (10 tables), memory freed each    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Notebook Sequence

Run the notebooks in the following order for complete validation:

### 1. `01_data_integrity_customer_uniqueness.ipynb`
**Purpose**: Validate fundamental data integrity constraints

**Key Checks**:
- Customer uniqueness across 320 chunk folders
- Per-customer isolation verification
- Split confirmation (train/valid/OOT)
- Temporal distribution analysis
- Sample count validation

**Outputs**:
- `chunk_summary_statistics.csv` - Chunk-level statistics
- `split_confirmation_statistics.csv` - Split validation results
- `validation_summary_report.csv` - Overall validation summary
- `temporal_distribution_plots.png` - Time series visualizations

**Critical Validation**: This notebook verifies that NO customer_id appears in multiple chunk folders. If violations are found, the modeling setup is broken.

**✅ Processing Note**: This notebook uses true incremental processing - loads one chunk at a time, never the full dataset. Memory usage is constant (~500 MB) regardless of `SAMPLING_RATIO`. Safe to run with 100% sampling on any cluster.

---

### 2. `02_target_analysis_by_split.ipynb`
**Purpose**: Analyze all 33 prediction head targets across splits and time

**Key Analyses**:
- Positive counts and rates per target (33 binary heads)
- Split comparison (train vs valid vs OOT)
- Temporal trends by effective month
- Class imbalance assessment
- Distribution shift detection

**Target Categories**:
- **Cards** (18 heads): Acquisitions, sub-products, account management
- **RESL** (6 heads): Mortgage, HELOC attrition/CPR
- **Unsecured Loan** (5 heads): ULON acquisitions, attrition
- **Unsecured LoC** (4 heads): ULOC acquisitions, attrition

**Outputs**:
- `target_statistics_by_split.csv` - Per-target stats by split
- `target_statistics_by_month.csv` - Monthly target statistics
- `class_imbalance_summary.csv` - Imbalance severity analysis
- `positive_rate_heatmap_by_split.png` - Heatmap visualization
- `temporal_trends_*.png` - Trend plots by category
- `split_comparison_analysis.csv` - Cross-split comparison

**✅ Processing Note**: This notebook uses true incremental processing - loads one chunk at a time, aggregates counts and sums incrementally. Memory usage is constant (~800 MB) regardless of `SAMPLING_RATIO`. Safe to run with 100% sampling on any cluster.

---

### 3. `03_feature_profiling.ipynb`
**Purpose**: Comprehensive feature profiling across all feature tables

**Tables Analyzed**:
- Customer: `cust_basic_summary`, `batch_credit_bureau`
- Account tables: `dem_acct`, `cc_acct`, `loc_acct`, `loan_acct`, `mtg_acct`, `inv_acct`
- Transaction tables: `dem_acct_trans`, `cc_acct_trans`

**Statistics Calculated** (per feature, per time period):
- Data type (numerical/categorical)
- Percentage of zeros
- Number of unique values
- Most frequent value and its percentage
- Percentiles: min, 1%, 50% (median), 99%, max
- Mean (for numerical features)

**Time-Period Analysis**:
- **Separate statistics** calculated for In-Time vs OOT periods
- Each feature has two rows in CSV: one for 'In-Time', one for 'OOT'
- Enables direct comparison of feature distributions between periods

**Visualizations**:
- Individual boxplots for each numerical feature comparing OOT vs in-time
- One PNG file per feature saved in table-specific folders
- Configurable sampling ratio (default 1%)

**Outputs**:
- `feature_profile_{family}_{table}.csv` - Per-table feature profiles with time_period column
  - Columns: `time_period`, `feature`, `data_type`, `pct_zero`, `n_unique`, `min`, `max`, `mean`, `median`, `p1`, `p99`, ...
  - Each feature appears twice: once for 'In-Time', once for 'OOT'
- `plots/{family}_{table}/{feature}.png` - Individual boxplot for each numerical feature

**⚠️ Processing Note**: This notebook loads full tables with sampling (one table at a time) to calculate accurate median and percentiles. These statistics **cannot be calculated incrementally**. Memory usage scales with `SAMPLING_RATIO`: 1%→~2GB, 100%→~20GB per table. Memory is freed between tables.

---

### 4. `04_drift_analysis_psi.ipynb`
**Purpose**: Comprehensive drift analysis for both numerical and categorical features

**Analysis Types**:

1. **PSI Drift Analysis (Numerical Features)**:
   - Overall drift: In-time vs OOT comparison
   - Monthly drift trends: Each OOT month vs in-time baseline
   - **PSI Thresholds**:
     - PSI < 0.1: Insignificant drift
     - 0.1 ≤ PSI < 0.25: Moderate drift (monitor)
     - PSI ≥ 0.25: Significant drift (investigate)

2. **Chi-Square Drift Analysis (Categorical Features)**:
   - Overall drift: In-time vs OOT comparison
   - Monthly drift trends: Each OOT month vs in-time baseline
   - **Chi-Square Thresholds**:
     - Chi-Square < 10.0: Insignificant drift
     - 10.0 ≤ Chi-Square < 25.0: Moderate drift (monitor)
     - Chi-Square ≥ 25.0: Significant drift (investigate)

3. **Monthly Statistics Trends (Numerical Features)**:
   - Median and average trends across ALL months (in-time + OOT)
   - One CSV per table with format: `feature_name, stat_method, month1, month2, ...`
   - Each feature has two rows (median and average)

**Outputs**:
- **Numerical Features (PSI)**:
  - `psi_overall_intime_vs_oot.csv` - PSI scores for all numerical features
  - `psi_monthly_trends_{table_name}.csv` - Monthly PSI evolution per table
  - `plots/monthly_trends/{table_name}/{feature}.png` - Individual trend plots
- **Categorical Features (Chi-Square)**:
  - `chi_square_overall_intime_vs_oot.csv` - Chi-square scores for all categorical features
  - `chi_square_monthly_trends_{table_name}.csv` - Monthly chi-square evolution per table
  - `plots/monthly_trends_chi_square/{table_name}/{feature}.png` - Individual trend plots
- **Monthly Statistics Trends**:
  - `monthly_statistics_trends_{table_name}.csv` - Median and average for all numerical features across all months
- **Summary Visualizations**:
  - `plots/psi_distribution.png` - PSI histogram and drift levels
  - `plots/chi_square_distribution.png` - Chi-square histogram and drift levels

**Flagged Features**: 
- Numerical features with PSI ≥ 0.25 are automatically flagged
- Categorical features with Chi-Square ≥ 25.0 are automatically flagged

**⚠️ Processing Note**: This notebook loads full tables with sampling (one table at a time) to calculate accurate drift metrics. Both PSI and Chi-Square require comparing complete distributions and **cannot be calculated incrementally**. Memory usage scales with `SAMPLING_RATIO`: 1%→~2GB, 100%→~15-20GB per table. Memory is freed between tables.

---

## Configuration Parameters

All notebooks include these configurable parameters in their configuration cells:

### Sampling Configuration
```python
SAMPLING_RATIO = 1.0          # 100% = full data, 0.01 = 1% sample
PLOT_SAMPLING_RATIO = 0.01    # For visualizations (notebooks 03, 04)
```
**Modify this** to control dataset size and memory usage.

### Data Paths (ADLS)
```python
DATA_PATH = "abfss://home@edaaaazepcalayelaye0001.dfs.core.windows.net/MD_Artifacts/money-out/data/"
TARGET_TRAIN_VAL_PATH = DATA_PATH + "target/cust/all_products_chunk_320/train_val/"
TARGET_TEST_PATH = DATA_PATH + "target/cust/all_products_chunk_320/test/"
OUTPUT_PATH = "abfss://...//mv/eda_validation/{analysis_type}/"
```
**Do not modify** unless data location changes.

### Time Split Configuration
```python
OOT_START_DATE = '2024-01-01'  # Out-of-time period starts from January 2024
```
**Do not modify** - this is the temporal split boundary.

### Chunk Configuration
```python
TOTAL_CHUNKS = 320              # Total number of chunk folders
TRAIN_CHUNKS = range(0, 256)    # 256 chunks for training customers
VALID_CHUNKS = range(256, 320)  # 64 chunks for validation customers
```
**Do not modify** - this is the data structure.

---

## Output Directory Structure

All outputs saved to ADLS (no DBFS required):

```
abfss://home@edaaaazepcalayelaye0001.dfs.core.windows.net/MD_Artifacts/money-out/mv/eda_validation/
├── data_integrity/
│   ├── chunk_summary_statistics.csv
│   ├── validation_summary_report.csv
│   ├── temporal_distribution_by_split.csv
│   └── temporal_distribution_plots.png
├── target_analysis/
│   ├── target_statistics_by_split.csv
│   ├── target_statistics_by_month.csv
│   ├── class_imbalance_summary.csv
│   ├── positive_rate_heatmap_by_split.png
│   └── temporal_trends_*.png (one per category)
├── feature_profiling/
    │   ├── feature_profile_{family}_{table}.csv (one per table, with In-Time and OOT rows)
    │   └── plots/
    │       └── {family}_{table}/
    │           └── {feature}.png (individual boxplot for each numerical feature)
└── drift_analysis/
    ├── psi_overall_intime_vs_oot.csv (numerical features)
    ├── chi_square_overall_intime_vs_oot.csv (categorical features)
    ├── psi_monthly_trends_{table_name}.csv (per table, numerical)
    ├── chi_square_monthly_trends_{table_name}.csv (per table, categorical)
    ├── monthly_statistics_trends_{table_name}.csv (per table, median/average trends)
    └── plots/
        ├── psi_distribution.png
        ├── chi_square_distribution.png
        ├── monthly_trends/
        │   └── {table_name}/
        │       └── {feature}.png (PSI trends for numerical features)
        ├── monthly_trends_chi_square/
        │   └── {table_name}/
        │       └── {feature}.png (Chi-square trends for categorical features)
```

---

## Key Constraints & Design Principles

### 1. Customer-Level Isolation
✓ **MAINTAINED**: Each customer_id appears in exactly ONE chunk folder  
✓ **VERIFIED**: Notebook 01 validates this constraint  
✗ **VIOLATION**: If violations found, modeling setup is invalid

### 2. Time Splits
- **Train**: Chunks 0-255, dates before 2024
- **Valid**: Chunks 256-319, dates before 2024
- **OOT**: All chunks, dates in 2024+

Rationale: Same customers can appear in both in-time and OOT periods, but in-time samples are split by customer into train/valid.

### 3. Target Integrity
- 33 binary prediction heads
- Multiple horizons (2-3m, 4-5m, 2-7m)
- Intentional blanking of certain month-ranges to avoid leakage
- **DO NOT** attempt to fix missing targets

### 4. Feature Tables
- Multiple source tables per month
- ~529 engineered features at training time
- Numerical features: normalized/quantized
- Categorical features: embedded (raw IDs in parquet)
- Sparse/missing values expected (e.g., account-specific features)

---

## Performance Considerations

### Memory Usage and Processing Strategy

These notebooks prioritize **statistical accuracy** while maintaining reasonable memory usage:

#### **Notebooks 01-02: True Incremental Processing**
- **What they calculate**: Counts, sums, customer tracking (can aggregate incrementally)
- **Processing**: One chunk at a time, never load full dataset
- **Memory usage**: ~100-500 MB (constant across all 320 chunks)
- **Data loaded**: Only `['pid', 'pred_dt']` + target columns

#### **Notebooks 03-04: Sampled Full-Table Processing**
- **What they calculate**: Median, percentiles, PSI, distributions (require complete data view)
- **Processing**: Load full table with sampling, one table at a time
- **Memory usage**: 
  - 1% sampling: ~500 MB - 2 GB per table
  - 10% sampling: ~1-5 GB per table
  - 100% sampling: ~5-20 GB per table (depending on table size)
- **Why**: Accurate median/percentiles impossible with chunk-by-chunk processing
- **Mitigation**: 
  - Process one table at a time (10 tables total)
  - Free memory between tables with `gc.collect()`
  - Use Spark's efficient Parquet reader 
  - Default 1% sampling keeps memory manageable

**Trade-off Decision**: We prioritize **statistical accuracy** over minimal memory usage. 
- Median/percentile approximations would introduce errors
- Feature drift (PSI) requires accurate distribution comparison
- 1% sampling provides good balance of accuracy and efficiency

### Sampling Strategy

All notebooks support configurable sampling:

```python
SAMPLING_RATIO = 1.0   # 100% - Full dataset (recommended for production)
SAMPLING_RATIO = 0.01  # 1% - Quick testing (recommended for development)
SAMPLING_RATIO = 0.001 # 0.1% - Ultra-fast testing
```

**Recommended Workflow**:
1. **Development**: Start with `SAMPLING_RATIO = 0.01` (1%) for quick iteration
2. **Validation**: Increase to `SAMPLING_RATIO = 0.1` (10%) for thorough testing
3. **Production**: Use `SAMPLING_RATIO = 1.0` (100%) for final validation

### Performance Tips

1. **Start with 1% sampling**: Always test notebooks 03-04 with `SAMPLING_RATIO = 0.01` first
2. **Monitor memory**: Check Databricks cluster metrics during notebook execution
3. **If memory issues occur**:
   - **For notebooks 01-02**: Should never have issues (true chunked processing)
   - **For notebooks 03-04**: Reduce `SAMPLING_RATIO` (e.g., 0.01 → 0.001)
4. **Gradual scale-up**: Test with 0.01 → 0.1 → 1.0 for notebooks 03-04 as you verify results
5. **Cluster recommendations** (for notebooks 03-04):
   - 1% sampling: Small cluster (8+ GB RAM)
   - 10% sampling: Medium cluster (16+ GB RAM)
   - 100% sampling: Large cluster (64+ GB RAM)
6. **Table-by-table processing**: Notebooks 03-04 process one table at a time, memory is freed between tables
7. **Progress monitoring**: Console output shows chunk/table processing progress

---

## Data Sources

All data paths are configured in each notebook's configuration cell.

### Feature Tables (Parquet Format)
Location: `{DATA_PATH}/feature/{table_name}/parquet`

**Customer Tables**:
- `cust_basic_sumary` - Customer-level demographic and summary features
- `batch_credit_bureau` - Credit bureau features (bimonthly updates)

**Account Tables** (per family):
- `dem_acct_2438` - Deposit account features
- `cc_acct_2444` - Credit card account features
- `loc_acct_2442` - Line of credit features
- `loan_acct_2439` - Loan account features
- `mtg_acct_2440` - Mortgage features
- `inv_acct_1331` - Investment features

**Transaction Tables** (aggregated to monthly):
- `dem_acct_trans_2438` - Deposit transaction aggregates
- `cc_acct_trans_2444` - Credit card transaction aggregates

### Target Data (CSV Format with Partitions)
Location: `{DATA_PATH}/target/cust/all_products_chunk_320/`

**Structure**:
```
train_val/
├── chunk=0/part-*.csv
├── chunk=1/part-*.csv
├── ...
└── chunk=319/part-*.csv

test/
├── chunk=0/part-*.csv
├── chunk=1/part-*.csv
├── ...
└── chunk=319/part-*.csv
```

**Splits**:
- `train_val/` - In-time data (customers split by chunk: 0-255 train, 256-319 valid)
- `test/` - OOT data (2024 effective dates, all chunks have OOT samples)

---

## Troubleshooting

### Common Issues

**Issue 1**: Out of Memory (Notebooks 03 or 04)
- **Cause**: Loading full table with high sampling ratio exceeds available memory
- **Solution**: Reduce `SAMPLING_RATIO` in the failing notebook:
  - Try 0.001 (0.1%) for very large tables
  - Or process fewer tables at once (comment out some tables in TABLES list)
- **Note**: Notebooks 01-02 use true chunked processing and should NOT have memory issues
- **Why it happens**: Notebooks 03-04 load full tables (sampled) to calculate accurate median/percentiles

**Issue 2**: Long Processing Time on Full Dataset
- **Cause**: Processing all 320 chunks sequentially (notebooks 01-02) or large tables (notebooks 03-04)
- **Solution**: Start with `SAMPLING_RATIO = 0.01` for initial testing
- **Note**: Processing time depends on cluster configuration and workload

**Issue 3**: Missing Columns Error
- **Cause**: Column names in metadata don't match actual data
- **Solution**: Check `feature_metadata.jsonl` matches table schema
- **Note**: Notebooks have try/except to skip missing columns

**Issue 4**: PSI Returns None
- **Causes**: Insufficient data in sample, constant features, all NaN values
- **Solution**: Increase `SAMPLING_RATIO` or check feature distribution
- **Action**: Review feature profiling (notebook 03) first

**Issue 5**: ADLS Permission Errors
- **Cause**: Insufficient permissions on ADLS path
- **Solution**: Verify you have write access to `MD_Artifacts/money-out/mv/` directory
- **Check**: Try creating test file with `dbutils.fs.put()`

**Issue 6**: Temp File Cleanup Warnings
- **Cause**: Temp files from previous runs not cleaned
- **Solution**: Warnings are harmless - files are automatically cleaned
- **Note**: Each operation creates new temp file and removes it

---

## Dependencies

### Required Packages
```python
# Core
pandas >= 1.5.0
numpy >= 1.23.0

# Visualization
matplotlib >= 3.6.0
seaborn >= 0.12.0

# Databricks utilities (for ADLS file operations)
pyspark >= 3.5.0  # Used only for reading Parquet files efficiently
dbutils  # Pre-installed in Databricks

# Standard library
json, pathlib, tempfile, os, gc, collections
```

All packages pre-installed in Databricks Runtime 14.3 ML.

### Note on Spark Usage and Data Loading

While these notebooks use Spark strategically:

**All Notebooks**:
- Read metadata JSONL with `spark.read.text()` (convenience)

**Notebooks 01-02** (Target validation):
- Load CSV chunks individually
- Process one chunk at a time with spark/pandas operations
- True incremental aggregation

**Notebooks 03-04** (Feature profiling & drift):
- Use Spark to read Parquet files (more efficient than pandas for Parquet)
- Apply sampling at Spark level: `spark.read.parquet().sample(fraction=0.01)`
- Convert to pandas when needed: `.toPandas()` for analysis
- **Important**: This loads the FULL SAMPLED table to ensure accurate median/percentile calculations

**Why This Hybrid Approach**:
- Spark excels at reading columnar Parquet format
- Sampling in Spark (distributed) is faster than in pandas
- Pandas excels at statistical operations once data is in memory
- Accurate median/percentiles require seeing all values (cannot be truly incremental)

---

## Validation Checklist

Use this checklist to ensure complete validation:

**Initial Testing** (with 1% sampling):
- [ ] Set `SAMPLING_RATIO = 0.01` in all notebooks
- [ ] Run Notebook 01 - Verify code executes successfully
- [ ] Run Notebook 02 - Check target statistics generation
- [ ] Run Notebook 03 - Verify feature profiling works
- [ ] Run Notebook 04 - Test PSI calculation
- [ ] Review outputs in ADLS directories

**Full Validation** (with 100% data):
- [ ] Set `SAMPLING_RATIO = 1.0` in all notebooks
- [ ] Run Notebook 01 - Data integrity validated
- [ ] **Critical**: Verify customer uniqueness (zero violations)
- [ ] Run Notebook 02 - All 33 targets analyzed
- [ ] Review class imbalance (highly imbalanced targets identified)
- [ ] Run Notebook 03 - Feature profiling complete
- [ ] Check feature statistics (anomalies flagged)
- [ ] Run Notebook 04 - Drift analysis complete
- [ ] Review PSI scores (significant drift features identified)

**Final Review**:
- [ ] Check all CSV files saved to ADLS
- [ ] Review all visualization PNG files
- [ ] Verify no customer chunk violations
- [ ] Document highly imbalanced targets
- [ ] Flag features with significant drift (PSI ≥ 0.25)
- [ ] Generate summary report for stakeholders

---

## How to Run

### Step-by-Step Execution

1. **Open Databricks Workspace**
   - Navigate to the notebooks folder
   - Ensure cluster is running (Runtime 14.3 ML or higher)

2. **Start with Notebook 01**
   - Open `01_data_integrity_customer_uniqueness.ipynb`
   - Review configuration cell (cell 3)
   - Adjust `SAMPLING_RATIO` if needed (start with 0.01 for testing)
   - Run all cells sequentially

3. **Check Outputs**
   - Navigate to ADLS output path in configuration
   - Verify CSV files and plots were created
   - Review validation results in notebook output

4. **Proceed to Notebooks 02, 03, 04**
   - Run in sequence
   - Check outputs after each notebook
   - Monitor memory usage (should stay stable due to chunked processing)

5. **Review Results**
   - Download CSV files from ADLS for analysis
   - Review visualizations
   - Document findings



**Key Points**:
- **Notebooks 01-02**: Memory constant regardless of sampling (true chunked processing)
- **Notebooks 03-04**: Memory scales with sampling (need full table for accurate stats)
- **Recommendation**: Use 1% sampling for notebooks 03-04 to get results with low memory

*Memory estimates approximate - vary based on actual table sizes and data characteristics*

---

## Contact & Support

For questions or issues:
1. Check this README first
2. Review notebook markdown cells for detailed explanations
3. Check inline code comments for technical details
4. Consult existing notebooks (`dataprep-feature-cleaning.ipynb`, `customer-dataload-example.ipynb`) for reference patterns
5. Review Databricks logs for error details
6. Verify ADLS permissions if file save errors occur

---

## Version History

**Version 1.0** (2025-10-31)
- Initial release
- Four comprehensive EDA notebooks
- Complete validation pipeline for Customer Foundation Model
- Memory-efficient chunked processing
- Configurable sampling ratios (1% to 100%)
- Direct ADLS file operations (no DBFS dependency)
- Comprehensive documentation and comments
- Well-organized with multiple heading levels

---

## Notes

1. **Do not modify data**: These notebooks are read-only analysis tools
2. **Preserve structure**: Do not aggregate across chunks randomly - customer isolation is critical
3. **Respect time splits**: Always separate in-time from OOT to avoid leakage
4. **Sample for speed**: Use 1% sampling for development, 100% for production validation
5. **Independent notebooks**: Each notebook can run independently (though sequential order recommended)
6. **Memory efficiency**: Chunked processing means even single-node clusters can handle full dataset
7. **ADLS only**: All outputs saved directly to ADLS using `dbutils.fs` (no local DBFS usage)

---

## Quick Start

```python
# 1. Open notebook 01_data_integrity_customer_uniqueness.ipynb
# 2. Adjust sampling if needed (cell 3):
SAMPLING_RATIO = 0.01  # Start with 1% for testing

# 3. Run all cells
# 4. Monitor console output for progress
# 5. Check outputs in ADLS:
#    abfss://...//mv/eda_validation/data_integrity/

# 6. Repeat for notebooks 02, 03, 04
```

---

**Happy Validating!

