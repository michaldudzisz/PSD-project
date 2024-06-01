import pandas as pd
import matplotlib.pyplot as plt

file_path = 'transactions.csv'
data = pd.read_csv(file_path)

data['timestamp'] = pd.to_datetime(data['timestamp'], format='mixed')

card_data = data[data['cardId'] == 101]

fig, ax = plt.subplots(1, 1, figsize=(10, 5))

low_value = 0.5
ax.scatter(card_data['timestamp'], card_data['value'], c=(card_data['value'] < low_value), cmap='coolwarm')
ax.axhline(y=low_value, color='y', linestyle='--')
ax.set_xlabel('Timestamp')
ax.set_ylabel('Value')
ax.set_title('Value vs Timestamp for cardId=101')
ax.grid(True)

plt.show()
