from bs4 import BeautifulSoup
from nltk.tokenize.regexp import RegexpTokenizer
from nltk.corpus import treebank
from nltk.tag import UnigramTagger
from nltk.chunk import RegexpParser
from redis_interface import RedisInterface
from time import sleep
from tagged_article import TaggedArticle

def removeApostrophe(string):
    return string.replace(u"\u2019", '')

def stringFromHTMLParagraph(paraWithTags):
    paraString = ''
    for taggedString in paraWithTags.strings:
        paraString += removeApostrophe(taggedString.string)
    return paraString

def titleFromArticleSoup(soup):
    titleDiv = soup.find(class_ = 'story-heading')
    if not titleDiv:
        titleDiv = soup.find(class_ = 'entry-title')
    return unicode(removeApostrophe(titleDiv.string))

# Set up the tokenizer and the tagger
tokenizer = RegexpTokenizer(r'\w+')
tagger = UnigramTagger(treebank.tagged_sents())

# Open up a redis connection
redisInterface = RedisInterface()

# Print status
print 'Reader ONLINE'

# Run the wait-execute loop
while True:

    while not redisInterface.hasPending():
        sleep(1)

    page = redisInterface.popPending()
    print 'Reading ' + page + ' STARTED'

    # Read the html page
    with open(page, 'r') as htmlPage:
        data = htmlPage.read().replace('\n', '');

    # Parse html
    soup = BeautifulSoup(data)
    articleTitle = titleFromArticleSoup(soup)
    articleBodyWithTags = soup.find_all('p', class_ = 'story-body-text')
    articleBody = [stringFromHTMLParagraph(p)
            for p in articleBodyWithTags]
    parasToProcess = [articleTitle] + articleBody

    print 'Title: ' + articleTitle

    # Tokenize and tag
    tokens = [tokenizer.tokenize(s) for s in parasToProcess]
    taggedArticleBody = [tagger.tag(t) for t in tokens]

    # Save to redis
    redisInterface.saveArticleData(
            TaggedArticle(articleTitle, taggedArticleBody,'article_data'))

    print 'Reading ' + page + ' FINISHED'
