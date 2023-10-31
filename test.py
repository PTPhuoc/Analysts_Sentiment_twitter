import matplotlib.pyplot as plt
import pandas as pd

data = pd.read_csv("D:/data/Adele.csv", header=0)
Comment = data["Comment"].replace(",", "", regex=True).astype(int).values
Retweet = data["Retweet"].replace(",", "", regex=True).astype(int).values
Quote = data["Quote"].replace(",", "", regex=True).astype(int).values
Heart = data["Heart"].replace(",", "", regex=True).astype(int).values
Day = data["Date Create Tweet"]
for i in range(len(Day)):
    Day[i] = Day[i].split(",")[0]

plt.figure(figsize=(10, 5))
plt.plot(Day, Comment, label="Comment")
plt.plot(Day, Retweet, label="Retweet")
plt.plot(Day, Quote, label="Quote")
plt.plot(Day, Heart, label="Heart")

plt.legend()
plt.show()