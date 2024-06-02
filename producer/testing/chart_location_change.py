import pandas as pd
import matplotlib.pyplot as plt

file_path = 'transactions.csv'
data = pd.read_csv(file_path)

data['timestamp'] = pd.to_datetime(data['timestamp'])

card_id = 7102

card_data = data[data['cardId'] == card_id]

if card_data.empty:
    print(f"No data available for cardId={card_id}")
else:
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    sc = ax.scatter(card_data['localization.longitude'], card_data['localization.latitude'],
                    c=card_data['timestamp'].apply(lambda x: x.timestamp()), cmap='viridis')

    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label('Timestamp')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title(f'Longitude vs Latitude for cardId={card_id}')
    ax.grid(True)

    plt.show()