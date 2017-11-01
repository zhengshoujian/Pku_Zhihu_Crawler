import zhihu_oauth
from zhihu_oauth.zhcls import SearchType
from zhihu_oauth import GetDataErrorException
import os
import time
import pymysql
import pymysql.cursors

class Pku_Zhihu(object):
    def __init__(self):
        self.client = zhihu_oauth.ZhihuClient()
        self.TokenFile = "token.pkl"
        self.connect = None
        self.cursor = None
        self.author_num = 0
        self.question_num = 0
        self.answer_count = 0

    def login(self):
        if os.path.isfile(self.TokenFile):
            self.client.load_token(self.TokenFile)
        else:
            self.client.login_in_terminal()
            self.client.save_token(self.TokenFile)

    def connectDatabase(self):
        try:
            self.connect = pymysql.connect(host='localhost', user='root', passwd='123456', db='zhihu', port=3306, charset='utf8')
            self.cursor = self.connect.cursor()
        except Exception as e:
            print ("异常")

    def clearTable(self):
        self.connectDatabase()
        try:
            self.cursor.execute("truncate table authors;")
            self.cursor.execute("truncate table questions")
            for i in range(10):
                answer_table_name = "answers" + str(i)
                self.cursor.execute("truncate table %s" % answer_table_name)
            self.connect.commit()
        except Exception:
            print ("清空表出错")
        self.closeDatabase()

    def closeDatabase(self):
        self.cursor.close()
        self.connect.close()

    def crawlQuestion(self, question):
        self.connectDatabase()
        #问题信息
        question_id = question.id
        title = question.title
        question_describe = question.detail
        updated_time = zhihu_oauth.ts2str(question.updated_time)
        answer_count = question.answer_count
        follower_count = question.follower_count
        #将问题插入问题表
        print ("正在爬取问题：" + title)
        print ("共" + str(answer_count) + "个回答")
        try:
            sql = "insert into questions values(%d, '%s', '%s', '%s', %d, %d);" % (question_id, title, question_describe, updated_time, answer_count, follower_count)
            #print (sql)
            self.cursor.execute(sql)
            self.connect.commit()
        except Exception as e:
            print ("插入问题表出错")
            print (e.with_traceback())
            self.closeDatabase()
            return
        else:
            self.question_num += 1
            print ("问题成功写入问题表")

        num = 0
        for answer in question.answers:
            if (num % 30 == 0):
                time.sleep(1)
            num += 1
            print("正在爬取第" + str(num) + "个回答：")
            #作者信息
            author_id = answer.author.id
            if ((author_id != 0) & (author_id != "0")):
                try:
                    author_name = answer.author.name
                except GetDataErrorException as e:
                    print ("获取用户名出错")
                    print (e.reason)
                    author_name = ""
                try:
                    gender = answer.author.gender
                    if gender == None:
                        gender = -1
                except GetDataErrorException as e:
                    print ("获取用户性别出错")
                    print (e.reason)
                    gender = -1
                try:
                    headline = answer.author.headline
                except GetDataErrorException as e:
                    print ("获取用户个性签名出错")
                    print (e.reason)
                    headline = ""
                try:
                    follower_count = answer.author.follower_count
                    thanked_count = answer.author.thanked_count
                    voteup_count = answer.author.voteup_count
                except GetDataErrorException as e:
                    print ("获取用户粉丝数等信息出错")
                    print (e.reason)
                    follower_count = -1
                    thanked_count = -1
                    voteup_count = -1
                try:
                    sql = "insert into authors values('%s', '%s', %d, '%s', %d, %d, %d);" % (author_id, author_name, gender, headline, follower_count, thanked_count, voteup_count)
                    print (sql)
                    self.cursor.execute(sql)
                    self.connect.commit()
                except Exception:
                    print ("插入作者表失败")
                    self.closeDatabase()
                    return
                else:
                    print ("作者信息成功写入作者表")
                    self.author_num += 1
            #回答信息
            answer_id = answer.id
            content = answer.content
            updated_time = zhihu_oauth.ts2str(answer.updated_time)
            voteup_count = answer.voteup_count
            thanks_count = answer.thanks_count

            content = content.replace("'", "''")

            answer_table_name = "answers" + str(answer_id % 9)


            # print (content)
            try:
                sql = "insert into %s values(%d, %d, '%s', '%s', '%s', %d, %d);" % (answer_table_name, answer_id, question_id, author_id, content, updated_time, voteup_count, thanks_count)
                #print (sql)
                self.cursor.execute(sql)
                self.connect.commit()
            except:
                print ("插入回答表出错:" + str(num))
                self.closeDatabase()
                return
            else:
                print ("爬取第" + str(num) + "个回答成功")
                self.answer_count += 1

        self.closeDatabase()

    def crawlQuestion1(self, question):
        self.connectDatabase()
        num = 0
        q_title = question.title
        print ("正在爬取问题：" + q_title)
        try:
            self.cursor.execute("drop table if exists %s;" % (q_title))
            self.cursor.execute("create table %s (author_id varchar(50), author varchar(30), gender int, datetime datetime, content text, voteup_count int, thanks_count int);" % (q_title))
            #self.connect.commit()
            #connect.close()
            print ("创建表" + q_title)
        except Exception as e:
            print ("创建表出错!")
            print (e.with_traceback())
            return

        for answer in question.answers:
            author_id = answer.author.id
            author = answer.author.name
            gender = answer.author.gender
            datetime = zhihu_oauth.ts2str(answer.updated_time)
            content = answer.content
            #comment_count = answer.comment_count
            voteup_count = answer.voteup_count
            thanks_count = answer.thanks_count
            num = num + 1
            #
            if gender == None:
                gender = -1
            content = content.replace("'","''")
            #print (content)
            print("第" + str(num) + "个回答：")
            #print (author_id, author, gender, datetime, thanks_count, voteup_count)
            try:
                sql = "insert into %s values('%s', '%s', %d, '%s', '%s', %d, %d)" % (q_title, author_id, author, gender,datetime, content, voteup_count, thanks_count)
                print (sql)
                self.cursor.execute(sql)
                self.connect.commit()
            except:
                print ("插入数据出错:" + str(num))
                self.closeDatabase()
                return
        self.closeDatabase()


    def crawlTopic(self, topic):
        print ("正在爬取话题： " + topic.name)
        print ("共" + str(topic.questions_count) + "个问题……")
        num = 1
        for question in topic.unanswered_questions:
            print("-------------------------------")
            print ("进行到第" + str(num) + "个问题：")
            #time.sleep(1)
            self.crawlQuestion(question)
            num += 1

    def test(self, tid):
        self.clearTable()
        topic = self.client.topic(tid)
        self.crawlTopic(topic)

    def crawl(self, query):
        self.clearTable()
        num = 0
        for result in self.client.search(query, SearchType.TOPIC):

            print("***********************************************************")
            print ("进行到第" + str(num) + "个话题")
            topic = result.obj
            self.crawlTopic(topic)
            time.sleep(5)

    def showResult(self):
        print ("共爬" + str(self.question_num) + "个问题")
        print (str(self.author_num) + "个作者")
        print (str(self.answer_count) + "个回答")


if __name__ == "__main__":
    zhihu = Pku_Zhihu()
    zhihu.login()
    zhihu.crawl("俄罗斯")
    zhihu.showResult()

