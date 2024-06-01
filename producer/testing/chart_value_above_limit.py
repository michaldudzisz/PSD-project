import pandas as pd
import matplotlib.pyplot as plt

file_path = 'transactions.csv'
data = pd.read_csv(file_path)

data['timestamp'] = pd.to_datetime(data['timestamp'], format='mixed')

card_100_data = data[data['cardId'] == 100]
card_7100_data = data[data['cardId'] == 7100]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12))

limit100 = card_100_data.iloc[0]["limit"]
ax1.scatter(card_100_data['timestamp'], card_100_data['value'], c=(card_100_data['value'] > limit100), cmap='coolwarm')
ax1.axhline(y=limit100, color='y', linestyle='--')
ax1.set_xlabel('Timestamp')
ax1.set_ylabel('Value')
ax1.set_title('Value vs Timestamp for cardId=100')
ax1.grid(True)

limit7100 = card_7100_data.iloc[0]["limit"]
ax2.scatter(card_7100_data['timestamp'], card_7100_data['value'], c=(card_7100_data['value'] > limit7100), cmap='coolwarm')
ax2.axhline(y=limit7100, color='y', linestyle='--')
ax2.set_xlabel('Timestamp')
ax2.set_ylabel('Value')
ax2.set_title('Value vs Timestamp for cardId=7100')
ax2.grid(True)

plt.tight_layout()

plt.show()
