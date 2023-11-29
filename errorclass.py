from datetime import datetime
import mariadb
import botfunction as func


class ErrorLog:
    def __init__(self, dbCursor=None, transactionDate=datetime.now().strftime("%Y-%m-%d"),
                 transactionTime=datetime.now().strftime("%H:%M:%S.%f"), moduleName=None, errorNo=None,
                 errorMessage=None, explanation=None):
        self.dbCursor = dbCursor
        self.transactionDate = transactionDate
        self.transactionTime = transactionTime
        self.moduleName = moduleName
        self.errorNo = errorNo
        self.errorMessage = errorMessage
        self.explanation = explanation

    def addData(self):
        if self.dbCursor is None:
            func.logError(f"{self.moduleName} {self.errorNo} {self.errorMessage}")
        else:
            try:
                self.transactionDate = datetime.now().strftime("%Y-%m-%d")
                self.transactionTime = datetime.now().strftime("%H:%M:%S.%f")
                self.dbCursor.execute(
                    "INSERT INTO errorlog (transaction_date, transaction_time, module_name, error_no, "
                    " error_message, explanation) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (self.transactionDate, self.transactionTime, self.moduleName, self.errorNo,
                     self.errorMessage, self.explanation))
            except mariadb.Error as ex:
                # print(f"Error: {ex.errno} {ex.errmsg}")
                func.logTraceback(ex)
