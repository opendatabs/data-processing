import pandas as pd
import great_expectations as ge

context = ge.get_context()

# Load your voting data (replace this with your file path)
voting_data = pd.read_excel('data/100399.xlsx')

# Create a Great Expectations DataFrame for validation
df_ge = ge.from_pandas(voting_data)

# 1. Expect valid election dates in 'Wahltermin'
df_ge.expect_column_values_to_match_strftime_format("Wahltermin", "%Y-%m-%d")

# 2. Expect the total votes not to exceed eligible voters
df_ge.expect_column_pair_values_A_to_be_less_than_or_equal_to_B(
    "Stimmen Total aus Wahlzettel", "Stimmberechtigte"
)

# 3. Check that the total number of eligible voters is the sum of eligible men and women
df_ge.expect_column_pair_values_to_be_equal(
    "Stimmberechtigte", "Stimmberechtigte Männer + Stimmberechtigte Frauen"
)

# 4. Ensure valid percentage format in 'Stimmbeteiligung' (without exceeding 100%)
df_ge.expect_column_values_to_match_regex("Stimmbeteiligung", r"^\d{1,3}\.\d{2}%$")
df_ge.expect_column_values_to_be_between("Stimmbeteiligung", min_value="0.00%", max_value="100.00%")

# Run the validation and get results
results = df_ge.validate()

# Print results
print(results)
