import random
import csv

def generate_and_save_random_data(file_path, num_rows=10):
    # Create a list to store the random data
    data = []

    # Generate and store random data in the list
    for _ in range(num_rows):
        random_integer = random.randint(1, 100)
        random_float = random.uniform(0.0, 1.0)
        random_element = random.choice(['A', 'B', 'C', 'D', 'E'])

        # Append the random data to the list as a tuple
        data.append((random_integer, random_float, random_element))

    # Write the data to the CSV file
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write the header row
        writer.writerow(["Random Integer", "Random Float", "Random Element"])
        # Write the data rows
        writer.writerows(data)

    return f"Random data saved to {file_path}"
