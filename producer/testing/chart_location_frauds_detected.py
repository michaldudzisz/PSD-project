import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import re


file_path = 'frauds.csv'
data = pd.read_csv(file_path)

# get desired fraud type only
pattern = r"Localization changed too fast.*"
data = data[data["fraudReason"].str.contains(pattern, regex=True)]

print(data.head)

data['transaction.timestamp'] = pd.to_datetime(data['transaction.timestamp']).apply(lambda x: x.timestamp() / (24 * 60 * 60))

card_id = 7102

card_data = data[data['transaction.cardId'] == card_id]

print(type(card_data['transaction.timestamp'].iloc[0]))

if card_data.empty:
    print(f"No data available for cardId={card_id}")
else:
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    sc = ax.scatter(card_data['transaction.localization.longitude'], card_data['transaction.localization.latitude'],
                    c=card_data['transaction.timestamp'], cmap='viridis')

    # sc = ax.scatter(card_data['localization.longitude'], card_data['localization.latitude'],
    #                 c=card_data['timestamp'].apply(lambda x: x.timestamp()), cmap='viridis')
    cbar = plt.colorbar(sc, ax=ax)

    cbar.ax.yaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    # cbar.set_ticks(np.linspace(np.min(date_values), np.max(date_values), 5))
    # cbar.update_ticks()

    cbar.set_label('Time of transaction')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title(f'Longitude vs Latitude for cardId={card_id}')
    ax.grid(True)

    plt.show()
