import pandas as pd

# Load CSVs
file1 = 'df_yield_weather.csv'
file2 = 'df_yield_weather_v2.csv'

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Step-by-step detailed checks
def compare_dataframes(df1, df2):
    # 1. Check shape
    if df1.shape != df2.shape:
        print(f"❌ Shape mismatch: {df1.shape} vs {df2.shape}")
        return False

    # 2. Check column names and order
    if not df1.columns.equals(df2.columns):
        print(f"❌ Column names/order mismatch:")
        print(f"df1 columns: {list(df1.columns)}")
        print(f"df2 columns: {list(df2.columns)}")
        return False

    # 3. Check data types
    if not (df1.dtypes == df2.dtypes).all():
        print("❌ Column dtype mismatch:")
        print(pd.DataFrame({'df1': df1.dtypes, 'df2': df2.dtypes}))
        return False

    # 4. Check values (with NaNs considered equal at same position)
    unequal_columns = []
    for col in df1.columns:
        s1 = df1[col]
        s2 = df2[col]
        if not s1.equals(s2):
            unequal_columns.append(col)

    if unequal_columns:
        print(f"❌ These columns differ: {unequal_columns}")
        for col in unequal_columns:
            mask = ~(df1[col].fillna('__NA__') == df2[col].fillna('__NA__'))
            if mask.any():
                print(f"\nColumn '{col}' differences:")
                diff_df = pd.DataFrame({
                    'row': df1.index[mask],
                    f'{file1}': df1[col][mask].values,
                    f'{file2}': df2[col][mask].values
                })
                print(diff_df.to_string(index=False))
        return False

    print("✅ All checks passed: The DataFrames are identical.")
    return True

# Run comparison
compare_dataframes(df1, df2)
