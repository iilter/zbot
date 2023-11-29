from datetime import datetime
import mariadb
import botfunction as func


class ErrorHelper:
    def __init__(self,
                 status_code=None,
                 code=None,
                 msg=None,
                 module=None,
                 url=None,
                 explanation=None):
        self.status_code = status_code
        self.code = code
        self.msg = msg
        self.module = module
        self.url = url
        self.explanation = explanation

    def addData(self):
        db = func.connectDB()
        dbCursor = db.cursor()

        if dbCursor is None:
            func.logError(f"{self.status_code} {self.code} {self.msg} {self.url}")
        else:
            trnDate = datetime.now().strftime("%Y-%m-%d")
            trnTime = datetime.now().strftime("%H:%M:%S")
            try:
                dbCursor.execute(
                    "INSERT INTO errorlog (transaction_date, transaction_time, module_name, url, status_code,"
                    "error_no, error_message, explanation) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (trnDate, trnTime, self.module, self.url, self.status_code,
                     self.code, self.msg, self.explanation))
            except mariadb.Error as ex:
                func.logTraceback(ex)

