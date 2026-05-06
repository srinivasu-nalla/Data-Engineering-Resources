from typing import List, Tuple
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

def deduplicate_by_keys(
    df: DataFrame, 
    key_cols: List[str], 
    order_cols_rules: List[Tuple[str, bool]]
) -> DataFrame:
    """
    Deduplicate a DataFrame using Window functions for complex sorting.
    """
    # 1. Build Sort Expressions dynamically
    order_exp = [
        F.col(c).asc() if asc else F.col(c).desc() 
        for c, asc in order_cols_rules
    ]

    # 2. Define Window Spec
    window_spec = (
        Window.partitionBy(*key_cols)
        .orderBy(*order_exp)
    )

    # 3. Transformation
    return (
        df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

"""
Usage Examples

Eg1 :
final_df = deduplicate_by_keys (
    order_df,
    ["user_id","order_date"],
    [("updated_at",True)]
)
final_df.show(truncate=False)


Eg2
final_df = deduplicate_by_keys (
    order_df,
    ["order_id"],
    [("updated_at",False)]
)
final_df.show(truncate=False)

"""
