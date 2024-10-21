def save_output(df, output_path):
    """Save DataFrame output to a specified path."""
    df.write.csv(output_path, header=True)
