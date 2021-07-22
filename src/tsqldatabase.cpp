/* Copyright (c) 2017-2019, AOYAMA Kazuharu
 * All rights reserved.
 *
 * This software may be used and distributed according to the terms of
 * the New BSD License, which is incorporated herein by reference.
 */
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#include "tsqldatabase.h"
#include "tsqldriverextension.h"
#include "tsystemglobal.h"
#include <QFileInfo>
#include <QMap>
#include <QReadWriteLock>


class TDatabaseDict : public QMap<QString, TSqlDatabase> {
public:
    mutable QReadWriteLock xlock;
};
Q_GLOBAL_STATIC(TDatabaseDict, dbDict)


TSqlDatabase::DbmsType TSqlDatabase::dbmsType() const
{
#if QT_VERSION >= 0x050400
    return (_sqlDatabase.driver()) ? (TSqlDatabase::DbmsType)_sqlDatabase.driver()->dbmsType() : UnknownDbms;
#else

    DbmsType dbms = UnknownDbms;
    const QString type = _sqlDatabase.driverName();
    switch (type[1].toLatin1()) {
    case 'P':
        if (type == QLatin1String("QPSQL") || type == QLatin1String("QPSQL7")) {
            dbms = PostgreSQL;
        }
        break;

    case 'M':
        if (type == QLatin1String("QMYSQL") || type == QLatin1String("QMYSQL3")) {
            dbms = MySqlServer;
        }
        break;

    case 'O':
        if (type == QLatin1String("QODBC") || type == QLatin1String("QODBC3")) {
            dbms = MSSqlServer;
            break;
        }
        if (type == QLatin1String("QOCI") || type == QLatin1String("QOCI8")) {
            dbms = Oracle;
        }
        break;

    case 'T':
        if (type == QLatin1String("QTDS") || type == QLatin1String("QTDS7")) {
            dbms = Sybase;
        }
        break;

    case 'D':
        if (type == QLatin1String("QDB2")) {
            dbms = DB2;
        }
        break;

    case 'S':
        if (type == QLatin1String("QSQLITE") || type == QLatin1String("QSQLITE2")) {
            dbms = SQLite;
        }
        break;

    case 'I':
        if (type == QLatin1String("QIBASE")) {
            dbms = Interbase;
        }
        break;

    default:
        break;
    }
    return dbms;
#endif
}


void TSqlDatabase::setDriverExtension(TSqlDriverExtension *extension)
{
    Q_ASSERT(!_driverExtension);
    _driverExtension = extension;
}


const TSqlDatabase &TSqlDatabase::database(const QString &connectionName)
{
    pid_t tid = gettid();
    tSystemDebug("TSqlDatabase::database tid: %d connectionName: %s", tid, connectionName.toStdString().c_str());

    static TSqlDatabase defaultDatabase;
    defaultDatabase._tid = tid;
    auto *dict = dbDict();
    QReadLocker locker(&dict->xlock);

    const int ms = 50;
    const struct timespec ts = {ms / 1000, (ms % 1000) * 1000 * 1000};

    if (dict->contains(connectionName)) {
        const int count = 4;
        int i = 0;
        while (i < count && !(*dict)[connectionName]._tid.testAndSetRelease(0, tid)) {
            tSystemDebug("Connection held by %d. Sleeping...", (*dict)[connectionName]._tid.loadAcquire());
            nanosleep(&ts, NULL);
            i++;
        }
        if (i == count) {
             tSystemDebug("Database not available");
             throw new std::runtime_error("Database not available");
        }
        tSystemDebug("Used by set to: %d", tid);
        return (*dict)[connectionName];
    } else {
        tSystemDebug("Returning defaultDatabase");
        return defaultDatabase;
    }
}

void TSqlDatabase::setInuse(const QString &connectionName)
{
    pid_t tid = gettid();
    tSystemDebug("TSqlDatabase::wait tid: %d connectionName: %s", tid, connectionName.toStdString().c_str());

    auto *dict = dbDict();
    QReadLocker locker(&dict->xlock);

    const int ms = 50;
    const struct timespec ts = {ms / 1000, (ms % 1000) * 1000 * 1000};

    if (dict->contains(connectionName) && (*dict)[connectionName]._tid.loadAcquire() != tid) {
        const int count = 16;
        int i = 0;
        while (i < count && !(*dict)[connectionName]._tid.testAndSetRelease(0, tid)) {
            tSystemDebug("setInuse connection held by %d. Sleeping...", (*dict)[connectionName]._tid.loadAcquire());
            nanosleep(&ts, NULL);
            i++;
        }
        if (i == count) {
             tSystemDebug("setInuse Database not available");
             throw new std::runtime_error("Database not available");
        }
        tSystemDebug("setInuse Used by set to: %d", tid);
    } else {
        tSystemDebug("setInuse connetion not found");
    }
}


TSqlDatabase &TSqlDatabase::addDatabase(const QString &driver, const QString &connectionName)
{
    pid_t tid = gettid();
    tSystemDebug("TSqlDatabase::addDatabase tid: %d connectionName: %s", tid, connectionName.toStdString().c_str());

    auto *dict = dbDict();
    QWriteLocker locker(&dict->xlock);

    if (dict->contains(connectionName)) {
        dict->take(connectionName);
    }

    TSqlDatabase db(QSqlDatabase::addDatabase(driver, connectionName), tid);
    dict->insert(connectionName, db);
    return (*dict)[connectionName];
}

const TSqlDatabase &TSqlDatabase::unsetInuse(const QString &connectionName)
{
    pid_t tid = gettid();
    tSystemDebug("TSqlDatabase::unsetInuse: %s tid: %d", connectionName.toStdString().c_str(), tid);
    static TSqlDatabase defaultDatabase;
    auto *dict = dbDict();
    QReadLocker locker(&dict->xlock);

    if (dict->contains(connectionName) && (*dict)[connectionName]._tid.loadAcquire() != 0) {
        tSystemDebug("Current tid: %d", (*dict)[connectionName]._tid.loadAcquire());
        if (!(*dict)[connectionName]._tid.testAndSetRelease(tid, 0)) {
             tSystemDebug("Violation of personal space: %d", (*dict)[connectionName]._tid.loadAcquire());
             return defaultDatabase;
        }
        return (*dict)[connectionName];
    }
    tSystemDebug("Connection name not found");
    return defaultDatabase;
}


void TSqlDatabase::removeDatabase(const QString &connectionName)
{
    auto *dict = dbDict();
    QWriteLocker locker(&dict->xlock);
    dict->take(connectionName);
    QSqlDatabase::removeDatabase(connectionName);
}


bool TSqlDatabase::contains(const QString &connectionName)
{
    auto *dict = dbDict();
    QReadLocker locker(&dict->xlock);
    return dict->contains(connectionName);
}


bool TSqlDatabase::isUpsertSupported() const
{
    return _driverExtension && _driverExtension->isUpsertSupported();
}
