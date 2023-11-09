import csv
import re
import pandas
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
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


def clean_data(Name, Tag_Name, Date, Tag_Tweet, Content_tweet,
               Comment, Retweet, Quote, Heart, Picture,
               Quote_links, Quote_name, Quote_Content):
    Name = remove_emoji(Name)
    Content_tweet = re.sub(r"[!?@$%^&~`*.,’'()_+=:;-]", "", Content_tweet.replace("\n", ""))
    Quote_Content = re.sub(r"[!?@$%^&~`*.,’()_+=:;-]", "", Quote_Content.replace("\n", ""))
    Comment = Comment.replace(" ", "").replace(",", "")
    Retweet = Retweet.replace(" ", "").replace(",", "")
    Quote = Quote.replace(" ", "").replace(",", "")
    Heart = Heart.replace(" ", "").replace(",", "")
    data = post_tweet(Name, Tag_Name, Date, Tag_Tweet, Content_tweet,
                      Comment, Retweet, Quote, Heart, Picture,
                      Quote_links, Quote_name, Quote_Content)
    print(data.Name + " " + data.Tag_Name + " " + data.Date + " " + data.Tag_Tweet + " " + remove_emoji(
        Content_tweet) + " " + data.Comment + " " + data.Retweet + " " + data.Quote + " " + data.Heart + " " + data.Picture + " " + data.Quote_Link + " " + data.Quote_Name + " " + remove_emoji(
        Quote_Content))
    print("Success Save Data!")
    save_data(data)


def save_list_tag_tweet(list_tweet):
    tag_tweet = ""
    for list in list_tweet:
        tag_tweet = tag_tweet + " " + list.text

    return tag_tweet


def crawler_data(posts, User_Name):
    for post in posts:
        dr.get(url + post.get("href"))
        page_post = dr.find_element(By.TAG_NAME, "html")
        soup_link = BeautifulSoup(page_post.get_attribute("innerHTML"), "html.parser")
        if soup_link.find("a", {"class", "fullname"}) is not None:
            Name = soup_link.find("a", {"class", "fullname"}).text
            Tag_Name = soup_link.find("a", {"class", "username"}).text
            Date = soup_link.find("span", {"class", "tweet-date"}).find("a").get("title")
            if soup_link.find("div", {"class", "tweet-content media-body"}).find("a") is None:
                Tag_Tweet = "No Tag"
            else:
                Tag_Tweet = soup_link.find("div", {"class", "tweet-content media-body"}).find_all("a")
                Tag_Tweet = save_list_tag_tweet(Tag_Tweet)
            Content_tweet = soup_link.find("div", {"class", "tweet-content media-body"}).text
            Tweet_State = soup_link.find("div", {"class", "tweet-stats"}).find_all("div", {"class", "icon-container"})
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
                if Qoute.find("div", {"class", "quote-text"}).text == "":
                    Quote_Content = "No Content"
                else:
                    Quote_Content = Qoute.find("div", {"class", "quote-text"}).text
            dr.get(url + "/" + "".join(User_Name))
            clean_data(Name, Tag_Name, str(Date), Tag_Tweet, Content_tweet,
                       Tweet_State[0].text, Tweet_State[1].text, Tweet_State[2].text, Tweet_State[3].text, Picture,
                       Quote_links, Quote_name, Quote_Content)
        else: dr.get(url + "/" + "".join(User_Name))

def save_data(Data):
    table = mydb["Posts"]
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
    with open("D:/data/data_tweet.csv", "a", encoding="utf-8",
              newline="") as files:
        writer_file = csv.writer(files)
        writer_file.writerow(
            [Data.Name, Data.Tag_Name, Data.Date, Data.Tag_Tweet, Data.Content_Tweet, Data.Comment, Data.Retweet,
             Data.Quote, Data.Heart, Data.Picture, Data.Quote_Link, Data.Quote_Name, Data.Quote_Content])


def select_user():
    with open("D:/data/data_tweet.csv", "w", encoding="utf-8",
              newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            ["Name", "Tag Name", "Date Create Tweet", "Tag Tweet", "Content Tweet", "Comment", "Retweet",
             "Quote", "Heart", "Image", "Quote Link", "Quote Tag Name", "Quote Content"])

    with open("D:/data/User.csv", "r") as file:
        read = pandas.read_csv(file, header=0)
        for i in read.iterrows():
            url_user = url + "/" + "".join(i[1].values)
            print("Select User: " + url_user)
            dr.get(url_user)
            get_page = dr.find_element(By.TAG_NAME, "html")
            soup = BeautifulSoup(get_page.get_attribute("innerHTML"), "html.parser")
            posts = soup.find_all("a", {"class", "tweet-link"})
            for m in range(5):
                try:
                    find = dr.find_element(By.LINK_TEXT, "Load more").text
                except:
                    find = ""

                if find != "":
                    dr.find_element(By.LINK_TEXT, "Load more").click()
                    get_page = dr.find_element(By.TAG_NAME, "html")
                    soup = BeautifulSoup(get_page.get_attribute("innerHTML"), "html.parser")
                    posts_more = soup.find_all("a", {"class", "tweet-link"})
                    posts.extend(posts_more)
            crawler_data(posts, i[1].values)
            print("Complete!")


url = "https://nitter.cz"
headers = {
    'authority': 'www.google.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
              'application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 '
                  'Safari/537.36',
}

dr = webdriver.Firefox()
wait = WebDriverWait(dr, 5)
dr.implicitly_wait(5)
dr.get(url)
no_bot = dr.find_element(By.TAG_NAME, "input")
no_bot.click()
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["data_tweet"]
select_user()
print("End Process!")
dr.quit()
