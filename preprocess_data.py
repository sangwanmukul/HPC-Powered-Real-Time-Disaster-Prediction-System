import pandas as pd
import numpy as np

# Load dataset
df = pd.read_csv("natural_disasters_2024.csv")

print("Original Columns:", df.columns.tolist())

# Strip column names
df.columns = df.columns.str.strip()

# Drop rows with missing 'Magnitude'
df = df.dropna(subset=['Magnitude'])

# Convert 'Magnitude' to numeric
df['Magnitude'] = pd.to_numeric(df['Magnitude'], errors='coerce')

# Generate dummy Latitude and Longitude (random for simulation)
np.random.seed(42)
df['Latitude'] = np.random.uniform(-90, 90, size=len(df))
df['Longitude'] = np.random.uniform(-180, 180, size=len(df))

# Create a synthetic 'Severity' value based on Magnitude + noise
df['Severity'] = df['Magnitude'] * np.random.uniform(0.8, 1.2, size=len(df))

# Keep relevant columns
df_cleaned = df[['Latitude', 'Longitude', 'Magnitude', 'Severity', 'Disaster_Type']]

# Drop NaNs if any remain
df_cleaned = df_cleaned.dropna()

# Save cleaned data
df_cleaned.to_csv("cleaned_disasters.csv", index=False)

print("✅ Cleaned and enriched data saved to cleaned_disasters.csv")
