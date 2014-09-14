import redis
import pickle

class RedisInterface:

    def __init__(self):
        self.handle = redis.StrictRedis()

    def saveArticleData(self, articleData, key):
        self.handle.rpush(key, pickle.dumps(articleData))

    def hasArticleData(self, articleNumber, key):
        if not self.handle.exists(key):
            return False
        return self.handle.llen(key) > articleNumber

    def getArticleData(self, articleNumber, key):
        return pickle.loads(
                self.handle.lindex(key, articleNumber))

    def putPending(self, pendingArticle):
        self.handle.rpush('pending', pendingArticle)

    def hasPending(self):
        if not self.handle.exists('pending'):
            return False
        return self.handle.llen('pending') != 0

    def popPending(self):
        return self.handle.lpop('pending')
