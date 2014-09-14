from redis_interface import RedisInterface
from nltk.chunk import RegexpParser
from operator import itemgetter
from time import sleep
from tagged_article import TaggedArticle

def sanitizeTags(taggedList):
    sanitizedList = []
    for key, value in taggedList:
        if not value:
            value = 'NNP'
        sanitizedList.append((key, value))
    return sanitizedList

# Open the redis interface
redisInterface = RedisInterface()

# Prepare the chunker
chunker = RegexpParser(r'''
        Nouns:
            {<JJ.*>*<NN.*>*}
        ''');

# Print status
print 'Analyzer ONLINE'

# Run the wait-execute loop
articleNumber = 0
while True:

    while not redisInterface.hasArticleData(articleNumber, 'article_data'):
        sleep(1)

    # Retreive the tagged data from redis
    taggedArticleObject = redisInterface.getArticleData(articleNumber, 'article_data')

    taggedArticleName = taggedArticleObject.articleTitle
    taggedArticleUnsanitized = taggedArticleObject.taggedData

    print 'Analyzing ' + taggedArticleName + ' STARTED'

    # Filter out strings with an invalid tag
    taggedArticle = [sanitizeTags(unsanitizedList)
            for unsanitizedList in taggedArticleUnsanitized]

    # Chunk and calculate frequency
    frequency = {}
    paraNumber = -1
    for para in taggedArticle:
        paraNumber += 1

        if not len(para):
            # Ignore empty paragraphs
            continue

        # Extract all subtrees tagged with the right identifier
        for subtree in chunker.parse(para).subtrees(
                filter = lambda x: x.node == 'Nouns'):

            # Concatenate member strings
            leafString = ' '.join(
                    [key.lower() for key, value in subtree.leaves()])

            # Get the increment value
            increment = 1
            if paraNumber == 0:
                increment = 3       # Title
            elif paraNumber == 1:
                increment = 2       # First paragraph

            # Increment the frequency of the current string
            if leafString in frequency:
                frequency[leafString] += increment
            else:
                frequency[leafString] = increment

    # Sort by value
    sortedChunks = sorted(frequency.iteritems(),
            key = itemgetter(1), reverse = True)

    # Display
    print sortedChunks[:3]
    print 'Analyzing FINISHED'

    # Increment the article number
    articleNumber += 1

