from PyQt5 import QtCore, QtGui, QtWidgets
import socket
import sys
from sqlalchemy import create_engine,text
import time
import threading
import json
import struct
import random

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(800, 600)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.layoutWidget = QtWidgets.QWidget(self.centralwidget)
        self.layoutWidget.setGeometry(QtCore.QRect(10, 490, 720, 30))
        self.layoutWidget.setObjectName("layoutWidget")
        self.horizontalLayout = QtWidgets.QHBoxLayout(self.layoutWidget)
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout.setObjectName("horizontalLayout")
        spacerItem = QtWidgets.QSpacerItem(518, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem)
        self.btnRefresh = QtWidgets.QPushButton(self.layoutWidget)
        self.btnRefresh.setObjectName("btnRefresh")
        self.horizontalLayout.addWidget(self.btnRefresh)
        self.btnQuit = QtWidgets.QPushButton(self.layoutWidget)
        self.btnQuit.setObjectName("btnQuit")
        self.horizontalLayout.addWidget(self.btnQuit)
        self.layoutWidget1 = QtWidgets.QWidget(self.centralwidget)
        self.layoutWidget1.setGeometry(QtCore.QRect(170, 10, 551, 461))
        self.layoutWidget1.setObjectName("layoutWidget1")
        self.verticalLayout_6 = QtWidgets.QVBoxLayout(self.layoutWidget1)
        self.verticalLayout_6.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_6.setObjectName("verticalLayout_6")
        self.horizontalLayout_2 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        self.label_12 = QtWidgets.QLabel(self.layoutWidget1)
        self.label_12.setObjectName("label_12")
        self.horizontalLayout_2.addWidget(self.label_12)
        self.editFilter = QtWidgets.QLineEdit(self.layoutWidget1)
        self.editFilter.setObjectName("editFilter")
        self.horizontalLayout_2.addWidget(self.editFilter)
        self.editFilter.textChanged.connect(self.filter_Data)
        self.verticalLayout_6.addLayout(self.horizontalLayout_2)
        self.table = QtWidgets.QTabWidget(self.layoutWidget1)
        self.table.setObjectName("table")
        self.tableyc = QtWidgets.QWidget()
        self.tableyc.setObjectName("tableyc")
        self.table.addTab(self.tableyc, "")
        self.tableyx = QtWidgets.QWidget()
        self.tableyx.setObjectName("tableyx")
        self.table.addTab(self.tableyx, "")
        self.tableyk = QtWidgets.QWidget()
        self.tableyk.setObjectName("tableyk")
        self.table.addTab(self.tableyk, "")
        self.tableyt = QtWidgets.QWidget()
        self.tableyt.setObjectName("tableyt")
        self.table.addTab(self.tableyt, "")
        self.verticalLayout_6.addWidget(self.table)
        self.layoutWidget2 = QtWidgets.QWidget(self.centralwidget)
        self.layoutWidget2.setGeometry(QtCore.QRect(20, 10, 121, 461))
        self.layoutWidget2.setObjectName("layoutWidget2")
        self.verticalLayout_7 = QtWidgets.QVBoxLayout(self.layoutWidget2)
        self.verticalLayout_7.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_7.setObjectName("verticalLayout_7")
        self.label = QtWidgets.QLabel(self.layoutWidget2)
        self.label.setObjectName("label")
        self.verticalLayout_7.addWidget(self.label)
        self.cbxRtu = QtWidgets.QComboBox(self.layoutWidget2)
        self.cbxRtu.setObjectName("cbxRtu")
        self.cbxRtu.addItem("")
        self.cbxRtu.addItem("")
        self.cbxRtu.addItem("")
        self.cbxRtu.addItem("")
        self.cbxRtu.addItem("")
        self.verticalLayout_7.addWidget(self.cbxRtu)
        spacerItem1 = QtWidgets.QSpacerItem(20, 40, QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Expanding)
        self.verticalLayout_7.addItem(spacerItem1)
        self.verticalLayout = QtWidgets.QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        self.label_2 = QtWidgets.QLabel(self.layoutWidget2)
        self.label_2.setObjectName("label_2")
        self.verticalLayout.addWidget(self.label_2)
        self.labelRtuId = QtWidgets.QLabel(self.layoutWidget2)
        self.labelRtuId.setObjectName("labelRtuId")
        self.verticalLayout.addWidget(self.labelRtuId)
        self.verticalLayout_7.addLayout(self.verticalLayout)
        self.verticalLayout_5 = QtWidgets.QVBoxLayout()
        self.verticalLayout_5.setObjectName("verticalLayout_5")
        self.label_10 = QtWidgets.QLabel(self.layoutWidget2)
        self.label_10.setObjectName("label_10")
        self.verticalLayout_5.addWidget(self.label_10)
        self.labelRtuName = QtWidgets.QLabel(self.layoutWidget2)
        self.labelRtuName.setObjectName("labelRtuName")
        self.verticalLayout_5.addWidget(self.labelRtuName)
        self.verticalLayout_7.addLayout(self.verticalLayout_5)
        self.verticalLayout_3 = QtWidgets.QVBoxLayout()
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        self.label_6 = QtWidgets.QLabel(self.layoutWidget2)
        self.label_6.setObjectName("label_6")
        self.verticalLayout_3.addWidget(self.label_6)
        self.labelRtuAddr = QtWidgets.QLabel(self.layoutWidget2)
        self.labelRtuAddr.setObjectName("labelRtuAddr")
        self.verticalLayout_3.addWidget(self.labelRtuAddr)
        self.verticalLayout_7.addLayout(self.verticalLayout_3)
        self.verticalLayout_4 = QtWidgets.QVBoxLayout()
        self.verticalLayout_4.setObjectName("verticalLayout_4")
        self.label_8 = QtWidgets.QLabel(self.layoutWidget2)
        self.label_8.setObjectName("label_8")
        self.verticalLayout_4.addWidget(self.label_8)
        self.labelRtuStatus = QtWidgets.QLabel(self.layoutWidget2)
        self.labelRtuStatus.setObjectName("labelRtuStatus")
        self.verticalLayout_4.addWidget(self.labelRtuStatus)
        self.verticalLayout_7.addLayout(self.verticalLayout_4)
        self.verticalLayout_2 = QtWidgets.QVBoxLayout()
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.label_4 = QtWidgets.QLabel(self.layoutWidget2)
        self.label_4.setObjectName("label_4")
        self.verticalLayout_2.addWidget(self.label_4)
        self.labelRtuTime = QtWidgets.QLabel(self.layoutWidget2)
        self.labelRtuTime.setObjectName("labelRtuTime")
        self.verticalLayout_2.addWidget(self.labelRtuTime)
        self.verticalLayout_7.addLayout(self.verticalLayout_2)
        spacerItem2 = QtWidgets.QSpacerItem(20, 28, QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Expanding)
        self.verticalLayout_7.addItem(spacerItem2)
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 800, 26))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        # Connect the Refresh, Quit, RTUs functions
        self.btnRefresh.clicked.connect(self.refreshData)
        self.btnQuit.clicked.connect(MainWindow.close)
        self.cbxRtu.currentIndexChanged.connect(self.selectRtu)

        self.retranslateUi(MainWindow)
        self.table.setCurrentIndex(3)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        self.btnRefresh.setText(_translate("MainWindow", "刷新"))
        self.btnQuit.setText(_translate("MainWindow", "退出"))
        self.label_12.setText(_translate("MainWindow", "过滤查询"))
        self.table.setWhatsThis(_translate("MainWindow", "<html><head/><body><p>遥测</p></body></html>"))
        self.table.setTabText(self.table.indexOf(self.tableyc), _translate("MainWindow", "遥测"))
        self.table.setTabText(self.table.indexOf(self.tableyx), _translate("MainWindow", "遥信"))
        self.table.setTabText(self.table.indexOf(self.tableyk), _translate("MainWindow", "遥控"))
        self.table.setTabText(self.table.indexOf(self.tableyt), _translate("MainWindow", "遥调"))
        self.label.setText(_translate("MainWindow", "RTU 信息"))
        self.cbxRtu.setItemText(0, _translate("MainWindow", "1"))
        self.cbxRtu.setItemText(1, _translate("MainWindow", "2"))
        self.cbxRtu.setItemText(2, _translate("MainWindow", "3"))
        self.cbxRtu.setItemText(3, _translate("MainWindow", "4"))
        self.cbxRtu.setItemText(4, _translate("MainWindow", "5"))
        self.label_2.setText(_translate("MainWindow", "RTU 序号"))
        self.labelRtuId.setText(_translate("MainWindow", "TextLabel"))
        self.label_10.setText(_translate("MainWindow", "刷新时间"))
        self.labelRtuName.setText(_translate("MainWindow", "TextLabel"))
        self.label_6.setText(_translate("MainWindow", "IP 地址与端口"))
        self.labelRtuAddr.setText(_translate("MainWindow", "TextLabel"))
        self.label_8.setText(_translate("MainWindow", "连接状态"))
        self.labelRtuStatus.setText(_translate("MainWindow", "TextLabel"))
        self.label_4.setText(_translate("MainWindow", "RTU 名称"))
        self.labelRtuTime.setText(_translate("MainWindow", "TextLabel"))

    def selectRtu(self, engine):
        # Implement the logic to handle the selection of an RTU
        selected_rtu = self.cbxRtu.currentText()

        # Fetch data based on the selected RTU
        self.fetch_data_for_rtu(engine, selected_rtu)

        # Update the tables with the fetched data
        self.updateTables()


    def refreshData(self, engine):
        # Get the currently selected RTU from the ComboBox
        selected_rtu = self.cbxRtu.currentText()

        # Fetch updated data for the selected RTU
        self.fetch_data_for_rtu(engine, selected_rtu)

        # Update the tables with the refreshed data
        self.updateTables()



    def filter_Data(self):
        # Get the filter text entered by the user
        filter_text = self.editFilter.text().strip()

        # Check if the filter text is empty
        if not filter_text:
            # If the filter text is empty, display all data in the tables
            self.updateTables()
            return

        # Initialize filtered data lists
        filtered_yc_data = []
        filtered_yx_data = []
        filtered_yk_data = []
        filtered_yt_data = []
        
        
        # Filter YC data
        for item in self.yc_data:
            if filter_text.lower() in str(item).lower():
                filtered_yc_data.append(item)

        # Filter YX data
        for item in self.yx_data:
            if filter_text.lower() in str(item).lower():
                filtered_yx_data.append(item)

        # Filter YK data
        for item in self.yk_data:
            if filter_text.lower() in str(item).lower():
                filtered_yk_data.append(item)

        # Filter YT data
        for item in self.yt_data:
            if filter_text.lower() in str(item).lower():
                filtered_yt_data.append(item)

        # Update the tables with the filtered data
        self.updateTables(filtered_yc_data, filtered_yx_data, filtered_yk_data, filtered_yt_data)


    def updateTables(self):
        self.tableyc.showEvent(self.yc_data)
        self.tableyx.showEvent(self.yx_data)
        self.tableyk.showEvent(self.yk_data)
        self.tableyt.showEvent(self.yt_data)

    def updateLabels(self,engine):
        if self.table == self.tableyc:
            with engine.connect() as sqldb:
                id = sqldb.execute(text("select id from rtu_yc_info"))
                name = sqldb.execute(text("select name from rtu_yc_info"))
                addr = sqldb.execute(text("select address from rtu_yc_info"))
                status = sqldb.execute(text("select status from rtu_yc_info"))
                time = sqldb.execute(text("select refresh_time from rtu_yc_info"))

                self.labelRtuId.setText(id)
                self.labelRtuName.setText(name)
                self.labelRtuAddr.setText(addr)
                self.labelRtuStatus.setText(status)
                self.labelRtuTime.setText(time)

        elif self.table == self.tableyx:
            with engine.connect() as sqldb:
                id = sqldb.execute(text("select id from rtu_yx_info"))
                name = sqldb.execute(text("select name from rtu_yx_info"))
                addr = sqldb.execute(text("select address from rtu_yx_info"))
                status = sqldb.execute(text("select status from rtu_yx_info"))
                time = sqldb.execute(text("select refresh_time from rtu_yx_info"))

                self.labelRtuId.setText(id)
                self.labelRtuName.setText(name)
                self.labelRtuAddr.setText(addr)
                self.labelRtuStatus.setText(status)
                self.labelRtuTime.setText(time)

        elif self.table == self.tableyk:
            with engine.connect() as sqldb:
                id = sqldb.execute(text("select id from rtu_yk_info"))
                name = sqldb.execute(text("select name from rtu_yk_info"))
                addr = sqldb.execute(text("select address from rtu_yk_info"))
                status = sqldb.execute(text("select status from rtu_yk_info"))
                time = sqldb.execute(text("select refresh_time from rtu_yk_info"))

                self.labelRtuId.setText(id)
                self.labelRtuName.setText(name)
                self.labelRtuAddr.setText(addr)
                self.labelRtuStatus.setText(status)
                self.labelRtuTime.setText(time)

        elif self.table == self.tableyt:
            with engine.connect() as sqldb:
                id = sqldb.execute(text("select id from rtu_yt_info"))
                name = sqldb.execute(text("select name from rtu_yt_info"))
                addr = sqldb.execute(text("select address from rtu_yt_info"))
                status = sqldb.execute(text("select status from rtu_yt_info"))
                time = sqldb.execute(text("select refresh_time from rtu_yt_info"))

                self.labelRtuId.setText(id)
                self.labelRtuName.setText(name)
                self.labelRtuAddr.setText(addr)
                self.labelRtuStatus.setText(status)
                self.labelRtuTime.setText(time)


    def fetch_data_for_rtu(self,engine,id):
        # fetch data for RTU
        with engine.connect() as sqldb:
            self.yc_data = sqldb.execute(text(f"select * from rtu_yc_info where id = {id} "))
            self.yx_data = sqldb.execute(text(f"select * from rtu_yx_info where id = {id} "))
            self.yk_data = sqldb.execute(text(f"select * from rtu_yk_info where id = {id} "))
            self.yt_data = sqldb.execute(text(f"select * from rtu_yt_info where id = {id} "))




if __name__ == "__main__":

    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())

    engine = create_engine(f"sqlite:///./db/rtu_{rtu_id}.db")