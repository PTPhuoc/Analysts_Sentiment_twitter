import csv
import re
import pandas
import requests
from bs4 import BeautifulSoup, Tag
import pymongo


class post_tweet:
    def __init__(self, Name, Tag_Name, Date, Tag_Tweet, Content_tweet, Comment, Retweet, Quote, Heart, Picture,
                 Quote_Link, Quote_Name, Quote_Content):
        self.Name = Name
        self.Tag_Name = Tag_Name
        self.Date = Date
        self.Tag_Tweet = Tag_Tweet
        self.Content_Tweet = Content_tweet
        self.Comment = Comment
        self.Retweet = Retweet
        self.Quote = Quote
        self.Heart = Heart
        self.Picture = Picture
        self.Quote_Link = Quote_Link
        self.Quote_Name = Quote_Name
        self.Quote_Content = Quote_Content


def remove_emoji(string):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002500-\U00002BEF"  # chinese char
                               u"\U00002702-\U000027B0"
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               u"\U0001f926-\U0001f937"
                               u"\U00010000-\U0010ffff"
                               u"\u2640-\u2642"
                               u"\u2600-\u2B55"
                               u"\u200d"
                               u"\u23cf"
                               u"\u23e9"
                               u"\u231a"
                               u"\ufe0f"  # dingbats
                               u"\u3030"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)


def set_stats(data):
    soup = BeautifulSoup()
    for i in range(len(data)):
        if data[i].text == "":
            data[i] = soup.new_tag("div")
            data[i].string = "0"


def crawler_data(posts, User_Name):
    for post in posts:
        req = requests.get(url + post.get("href"), headers=headers)
        soup_link = BeautifulSoup(req.text, "html.parser")
        Name = soup_link.find("a", {"class", "fullname"})
        Tag_Name = soup_link.find("a", {"class", "username"})
        Date = soup_link.find("span", {"class", "tweet-date"}).find("a").get("title")
        Tag_Tweet = soup_link.find("div", {"class", "tweet-content media-body"}).find("a")
        if Tag_Tweet is None:
            Tag_Tweet = "No Tag"
        else:
            Tag_Tweet = Tag_Tweet.text

        Content_tweet = soup_link.find("div", {"class", "tweet-content media-body"}).text.replace("’", "").replace(",", "").replace("\n", "")
        if Content_tweet == "":
            Content_tweet = "No Content"

        Tweet_State = soup_link.find("div", {"class", "tweet-stats"}).find_all("div", {"class", "icon-container"})
        set_stats(Tweet_State)
        Image = soup_link.find("div", {"class", "tweet-body"}).find("div", {"class", "attachments"})
        if Image is None:
            Picture = "No Image"
        elif Image.find("a", {"class", "still-image"}) is None:
            Picture = "No Image"
        else:
            Image = Image.find("a", {"class", "still-image"}).get("href")
            Picture = url + str(Image)

        Qoute = soup_link.find("div", {"class", "tweet-body"}).find("div", {"class", "quote quote-big"})
        if Qoute is None:
            Quote_links = "No Qoute"
            Quote_name = "No Name"
            Quote_Content = "No Content"
        else:
            Qoute_link = Qoute.find("a").get("href")
            Quote_links = url + Qoute_link
            Quote_name = Qoute.find("a", {"class", "username"}).text
            if Qoute.find("div", {"class", "quote-text"}) is None:
                Quote_Content = "No Content"
            else:
                Quote_Content = Qoute.find("div", {"class", "quote-text"}).text.replace("’", "").replace(",", "").replace("\n", "")
        data = post_tweet(Name.text, Tag_Name.text, str(Date), Tag_Tweet, remove_emoji(Content_tweet),
                          Tweet_State[0].text, Tweet_State[1].text, Tweet_State[2].text, Tweet_State[3].text, Picture,
                          Quote_links, Quote_name, remove_emoji(Quote_Content))
        save_data(data, User_Name)
        print(data.Name + " " + data.Tag_Name + " " + data.Date + " " + data.Tag_Tweet + " " + remove_emoji(
            Content_tweet) + " " + data.Comment + " " + data.Retweet + " " + data.Quote + " " + data.Heart + " " + data.Picture + " " + data.Quote_Link + " " + data.Quote_Name + " " + remove_emoji(
            Quote_Content))
        print("Success Save Data!")


def save_data(Data, User_Name):
    #Nếu lưu trên mongodb thì mở cái này ra
    table = mydb["".join(User_Name)]
    data_form = {
       "Name": Data.Name,
       "Tag Name": Data.Tag_Name,
       "Date Create Tweet": Data.Date,
       "Tag Tweet": Data.Tag_Tweet,
       "Content Tweet": Data.Content_Tweet,
       "Comment": Data.Comment,
       "Retweet": Data.Retweet,
       "Quote": Data.Quote,
       "Heart": Data.Heart,
       "Image": Data.Picture,
       "Quote Link": Data.Quote_Link,
       "Quote Tag Name": Data.Quote_Name,
       "Quote Content": Data.Quote_Content
       }
    table.insert_one(data_form)

    # Nếu lưu trên mongodb rồi thì đóng hết mấy cái lệnh dưới này. Nhớ đổi đường dẩn file nếu lưu trên máy
    with open("D:/data/" + "".join(User_Name) + ".csv", "a", encoding="utf-8",
              newline="") as files:
        writer_file = csv.writer(files)
        writer_file.writerow(
            [Data.Name, Data.Tag_Name, Data.Date, Data.Tag_Tweet, Data.Content_Tweet, Data.Comment, Data.Retweet,
             Data.Quote, Data.Heart, Data.Picture, Data.Quote_Link, Data.Quote_Name, Data.Quote_Content])


def select_user():
    # Nhớ đổi đường dẩn file nếu lưu trên máy, chổ này dùng để lấy tên các người nổi tiếng nên lấy tác tag @name
    with open("D:/data/User.csv", "r") as file:
        read = pandas.read_csv(file, header=0)
        for i in read.iterrows():
            url_user = url + "/" + "".join(i[1].values)
            print("Select User: " + url_user)
            req = requests.get(url_user, headers=headers)
            soup = BeautifulSoup(req.text, "html.parser")
            posts = soup.find_all("a", {"class", "tweet-link"})
            # Nhớ đổi đường dẩn file nếu lưu trên máy
            with open("D:/data/" + "".join(i[1].values) + ".csv", "w", encoding="utf-8",
                      newline="") as file:
                writer = csv.writer(file)
                writer.writerow(
                    ["Name", "Tag Name", "Date Create Tweet", "Tag Tweet", "Content Tweet", "Comment", "Retweet",
                     "Quote",
                     "Heart", "Image", "Quote Link", "Quote Tag Name", "Quote Content"])
            crawler_data(posts, i[1].values)
            print("Complete!")


url = "https://nitter.net"
headers = {
    'authority': 'www.google.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
              'application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 '
                  'Safari/537.36',
}
# Ở đây dùng để lưu dữ liệu lên mongodb
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["data_tweet"]

select_user()
print("End Process!")
