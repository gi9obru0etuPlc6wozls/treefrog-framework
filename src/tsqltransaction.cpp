/* Copyright (c) 2011-2019, AOYAMA Kazuharu
 * All rights reserved.
 *
 * This software may be used and distributed according to the terms of
 * the New BSD License, which is incorporated herein by reference.
 */

#include "tsqldatabasepool.h"
#include <QSqlDriver>
#include <TSqlTransaction>
#include <TSystemGlobal>
#include <TWebApplication>

#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)


/*!
  \class TSqlTransaction
  \brief The TSqlTransaction class provides a transaction of database.
*/


TSqlTransaction::TSqlTransaction()
{
}


TSqlTransaction::~TSqlTransaction()
{
    rollback();
}


bool TSqlTransaction::begin()
{
    pid_t tid = gettid();
    tSystemDebug("TSqlTransaction::begin tid: %d connectionName: %s", tid, tdatabase().sqlDatabase().connectionName().toStdString().c_str());

    TSqlDatabase::setInuse(tdatabase().sqlDatabase().connectionName());

    if (Q_UNLIKELY(!tdatabase().sqlDatabase().isValid())) {
        tSystemError("Can not begin transaction. Invalid database: %s", qPrintable(tdatabase().sqlDatabase().connectionName()));
        return false;
    }

    if (!_enabled) {
        return true;
    }

    if (!tdatabase().sqlDatabase().driver()->hasFeature(QSqlDriver::Transactions)) {
        return true;
    }

    if (_active) {
        tSystemDebug("Has begun transaction already. database:%s", qPrintable(tdatabase().sqlDatabase().connectionName()));
        return true;
    }

    _active = tdatabase().sqlDatabase().transaction();
    _connectionName = tdatabase().sqlDatabase().connectionName();
    int id = TSqlDatabasePool::getDatabaseId(tdatabase().sqlDatabase());
    if (Q_LIKELY(_active)) {
        Tf::traceQueryLog("[BEGIN] [databaseId:%d] %s", id, qPrintable(_connectionName));
    } else {
        Tf::traceQueryLog("[BEGIN Failed] [databaseId:%d] %s", id, qPrintable(_connectionName));
    }
    return _active;
}


bool TSqlTransaction::commit()
{
    bool res = true;

    if (!_enabled) {
        return res;
    }

    if (_active) {
        if (!tdatabase().sqlDatabase().isValid()) {
            tSystemWarn("Database is invalid. [%s]  [%s:%d]", qPrintable(_connectionName), __FILE__, __LINE__);
        } else {
            res = tdatabase().sqlDatabase().commit();

            int id = TSqlDatabasePool::getDatabaseId(tdatabase().sqlDatabase());
            if (Q_LIKELY(res)) {
                Tf::traceQueryLog("[COMMIT] [databaseId:%d] %s", id, qPrintable(tdatabase().sqlDatabase().connectionName()));
            } else {
                Tf::traceQueryLog("[COMMIT Failed] [databaseId:%d] %s", id, qPrintable(tdatabase().sqlDatabase().connectionName()));
            }
        }
    }

    _active = false;
    return res;
}


bool TSqlTransaction::rollback()
{
    bool res = true;

    if (!_enabled) {
        return res;
    }

    if (_active) {
        if (!tdatabase().sqlDatabase().isValid()) {
            tSystemWarn("Database is invalid. [%s]  [%s:%d]", qPrintable(_connectionName), __FILE__, __LINE__);
        } else {
            res = tdatabase().sqlDatabase().rollback();

            int id = TSqlDatabasePool::getDatabaseId(tdatabase().sqlDatabase());
            if (Q_LIKELY(res)) {
                Tf::traceQueryLog("[ROLLBACK] [databaseId:%d] %s", id, qPrintable(tdatabase().sqlDatabase().connectionName()));
            } else {
                Tf::traceQueryLog("[ROLLBACK Failed] [databaseId:%d] %s", id, qPrintable(tdatabase().sqlDatabase().connectionName()));
            }
        }
    }

    _active = false;
    return res;
}

void TSqlTransaction::setCommonName(const QString &commonName)
{
    tSystemDebug("TSqlTransaction::setCommonName: %s", commonName.toStdString().c_str());
    _commonName = commonName;
}

const QString &TSqlTransaction::commonName() const {
    return _commonName;
}
