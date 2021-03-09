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
    mutable QReadWriteLock lock;
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
    tSystemDebug("TSqlDatabase::database tid: %ld", gettid());

    static TSqlDatabase defaultDatabase;
    auto *dict = dbDict();
    QReadLocker locker(&dict->lock);

    int ms = 5;
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000 * 1000};

    if (dict->contains(connectionName)) {
        while ((*dict)[connectionName].usedBy != 0 && (*dict)[connectionName].usedBy != gettid()) {
            tSystemDebug("Sleeping Used by: %d", (*dict)[connectionName].usedBy);
            nanosleep(&ts, NULL);
        };
        (*dict)[connectionName].usedBy = gettid();
        tSystemDebug("Used by set to: %d", (*dict)[connectionName].usedBy);
        return (*dict)[connectionName];
    } else {
        return defaultDatabase;
    }
}


TSqlDatabase &TSqlDatabase::addDatabase(const QString &driver, const QString &connectionName)
{
    tSystemDebug("TSqlDatabase::addDatabase");

    TSqlDatabase db(QSqlDatabase::addDatabase(driver, connectionName));
    auto *dict = dbDict();
    QWriteLocker locker(&dict->lock);

    if (dict->contains(connectionName)) {
        dict->take(connectionName);
    }

    dict->insert(connectionName, db);
    (*dict)[connectionName].usedBy = gettid();
    return (*dict)[connectionName];
}

const TSqlDatabase &TSqlDatabase::unsetInuse(const QString &connectionName)
{
    tSystemDebug("TSqlDatabase::unsetInuse: %s tid: %ld", connectionName.toStdString().c_str(), gettid());
    static TSqlDatabase defaultDatabase;
    auto *dict = dbDict();
    QReadLocker locker(&dict->lock);

    if (dict->contains(connectionName)) {
        tSystemDebug("TSqlDatabase::unsetInuse Used by: %d", (*dict)[connectionName].usedBy);

        if ((*dict)[connectionName].usedBy == gettid()) {
            tSystemDebug("usedBy = 0");
            (*dict)[connectionName].usedBy = 0;
            return (*dict)[connectionName];
        }
        tSystemDebug("Violation of personal space");
        return defaultDatabase;
    }
    tSystemDebug("Connection name not found");
    return defaultDatabase;
}


void TSqlDatabase::removeDatabase(const QString &connectionName)
{
    auto *dict = dbDict();
    QWriteLocker locker(&dict->lock);
    dict->take(connectionName);
    QSqlDatabase::removeDatabase(connectionName);
}


bool TSqlDatabase::contains(const QString &connectionName)
{
    auto *dict = dbDict();
    QReadLocker locker(&dict->lock);
    return dict->contains(connectionName);
}


bool TSqlDatabase::isUpsertSupported() const
{
    return _driverExtension && _driverExtension->isUpsertSupported();
}
