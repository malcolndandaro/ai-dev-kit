# Template Guide

## Overview

This guide covers the synthetic data generation template structure and how to customize it.

---

## Template

Use the **[generate_data_template.py](generate_data_template.py)** as your starting point.

**Purpose**: Faker-based template with customer and order examples

**Use when**:

- Need example of relationships and foreign keys
- Building multi-table datasets
- Want realistic data with minimal setup

**Tables included**:

- Customers (with addresses, emails, phone numbers)
- Orders (with customer relationships)

**Customization approach**:

- Copy the template as `generate_data.py` in your project
- Modify table generation functions
- Add new tables as needed

---

## Template Structure

Standard structure of `generate_data.py`:

```python
# Standard structure:
# 1. Imports (Faker, pandas, pathlib)
# 2. Configuration (SCALE_FACTOR, OUTPUT_PATH)
# 3. Table generation functions using Faker
# 4. Data generation execution
# 5. Parquet file writing
```

---

## Customization Patterns

### Pattern 1: Modify Existing Tables

TODO: Examples of modifying column definitions, distributions, etc.

### Pattern 2: Add New Tables

TODO: Examples of adding new tables with relationships

### Pattern 3: Custom Distributions

TODO: Examples of custom data distributions

---

## Environment Variables

Templates should read these environment variables:

- `SCALE_FACTOR`: Float multiplier for row counts (default: 1.0)
- `OUTPUT_PATH`: Base path for output files (local or Volume path)

```python
import os

scale_factor = float(os.environ.get('SCALE_FACTOR', 1.0))
output_path = os.environ.get('OUTPUT_PATH', './data')
num_rows = int(10000 * scale_factor)
```

---

## Output Structure

Templates should write parquet files in this structure:

```
data/
├── table1/
│   ├── part-00000.parquet
│   ├── part-00001.parquet
│   └── ...
├── table2/
│   └── part-00000.parquet
└── ...
```

---

## TODO: Complete This Guide

This is a placeholder structure. Complete with:

1. Detailed breakdown of story template tables and columns
2. Examples of common customizations
3. Best practices for table relationships
4. Performance considerations for large datasets
5. Schema evolution patterns
