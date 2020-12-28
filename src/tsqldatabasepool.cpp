/* Copyright (c) 2010-2019, AOYAMA Kazuharu
 * All rights reserved.
 *
 * This software may be used and distributed according to the terms of
 * the New BSD License, which is incorporated herein by reference.
 */

#include "tsqldatabasepool.h"
#include "tsqldatabase.h"
#include "tsqldriverextensionfactory.h"
#include "tsystemglobal.h"
#include <TWebApplication>
#include <TSqlQuery>
#include <TAppSettings>
#include <QMutexLocker>
#include <QFileInfo>
#include <QDir>
#include <ctime>

constexpr auto CONN_NAME_FORMAT = "rdb%02d_%d";
constexpr auto CONN_COMMONNAME_FORMAT = "udb%02d_%s";


TSqlDatabasePool *TSqlDatabasePool::instance()
{
    //tSystemDebug("TSqlDatabasePool::instance");
    static TSqlDatabasePool *databasePool = []() {
        auto *pool = new TSqlDatabasePool;
        pool->maxConnects = Tf::app()->maxNumberOfThreadsPerAppServer();
        pool->init();
        return pool;
    }();
    return databasePool;
}


TSqlDatabasePool::TSqlDatabasePool() : QObject()
{ }


TSqlDatabasePool::~TSqlDatabasePool()
{
    timer.stop();

    for (int j = 0; j < Tf::app()->sqlDatabaseSettingsCount(); ++j) {
        auto &cache = cachedDatabase[j];
        QString name;
        while (cache.pop(name)) {
            QSqlDatabase db = TSqlDatabase::database(name).sqlDatabase();
            db.close();
            TSqlDatabase::removeDatabase(name);
        }

        auto &stack = availableNames[j];
        while (stack.pop(name)) {
            TSqlDatabase::removeDatabase(name);
        }
    }

    delete[] cachedDatabase;
    delete[] lastCachedTime;
    delete[] availableNames;
}


static QString driverType(int databaseId)
{
    auto settings = Tf::app()->sqlDatabaseSettings(databaseId);
    QString key = QLatin1String("DriverType");
    QString type = settings.value(key).toString().trimmed();

    if (type.isEmpty()) {
        tWarn() << "Empty parameter: " << key << " databaseId:" << databaseId;
    }
    return type;
}


void TSqlDatabasePool::init()
{
    if (Tf::app()->sqlDatabaseSettingsCount() == 0) {
        tSystemWarn("SQL database not available");
        return;
    }

    tSystemDebug("+++ SQL database is available");
    userDbCache = new TLruCache(3);   // TODO: allocate for each database

    cachedDatabase = new TStack<QString>[Tf::app()->sqlDatabaseSettingsCount()];
    lastCachedTime = new TAtomic<uint>[Tf::app()->sqlDatabaseSettingsCount()];
    availableNames = new TStack<QString>[Tf::app()->sqlDatabaseSettingsCount()];
    bool aval = false;
    tSystemDebug("SQL database available");

    // Adds databases previously
    for (int j = 0; j < Tf::app()->sqlDatabaseSettingsCount(); ++j) {
        auto settings = Tf::app()->sqlDatabaseSettings(j);

        QString type = driverType(j);
        if (type.isEmpty()) {
            continue;
        }
        aval = true;

        if (!settings.value("commonName").isNull()) {
            continue;
        }

        auto &stack = availableNames[j];
        for (int i = 0; i < maxConnects; ++i) {
            TSqlDatabase &db = TSqlDatabase::addDatabase(type, QString().sprintf(CONN_NAME_FORMAT, j, i));
            if (!db.isValid()) {
                tWarn("Parameter 'DriverType' is invalid");
                break;
            }

            setDatabaseSettings(db, j);
            stack.push(db.connectionName());  // push onto stack
            tSystemDebug("Add Database successfully. name:%s", qPrintable(db.connectionName()));
        }
    }

    if (aval) {
        // Starts the timer to close extra-connection
        tSystemDebug("Timer started");
        timer.start(10000, this);
    }
}


QSqlDatabase TSqlDatabasePool::x_database(int databaseId, const QString &commonName)
{
//    TSqlDatabase tdb;

    if (Q_UNLIKELY(databaseId < 0 || databaseId >= Tf::app()->sqlDatabaseSettingsCount())) {
        throw RuntimeException("No pooled connection", __FILE__, __LINE__);
    }

    if (!commonName.isEmpty()) {
        tSystemDebug("*** Ooooweee");

        QString type = driverType(databaseId);
        if (type.isEmpty()) {
            tSystemError("SQL database database type is null: %d", databaseId);
            return QSqlDatabase();
        }
        tSystemDebug("SQL database database type: %s", type.toStdString().c_str());

        auto dbName = QString().sprintf(CONN_COMMONNAME_FORMAT, databaseId, commonName.toLatin1().constData());
        tSystemDebug("Connection name: %s", dbName.toLatin1().constData());

        TSqlDatabase xdb = TSqlDatabase::database(dbName);
        if (xdb.connectionName().isEmpty()) {
            xdb = TSqlDatabase::addDatabase(type, dbName);
            setCertAuthSettings(xdb, databaseId, commonName);
            tSystemDebug("Add Database successfully. name:%s", qPrintable(xdb.connectionName()));
        }

        if (Q_UNLIKELY(!xdb.sqlDatabase().open())) {
            tError("Database open error. Invalid database settings, or maximum number of SQL connection exceeded.");
            tSystemError("SQL database open error: %s %s", qPrintable(xdb.sqlDatabase().connectionName()),
                         qPrintable(xdb.sqlDatabase().lastError().text()));
            return QSqlDatabase();
        }

        userDbCache->cache(dbName);

        tSystemDebug("SQL database opened successfully (env:%s)", qPrintable(Tf::app()->databaseEnvironment()));
        tSystemDebug("Gets database: %s", qPrintable(xdb.sqlDatabase().connectionName()));
        return xdb.sqlDatabase();
    } else {
        auto &cache = cachedDatabase[databaseId];
        auto &stack = availableNames[databaseId];

        for (;;) {
            QString name;
            if (cache.pop(name)) {
                auto tdb = TSqlDatabase::database(name);
                if (Q_LIKELY(tdb.sqlDatabase().isOpen())) {
                    tSystemDebug("Gets cached database: %s", qPrintable(tdb.connectionName()));
                    return tdb.sqlDatabase();
                } else {
                    tSystemError("Pooled database is not open: %s  [%s:%d]", qPrintable(tdb.connectionName()),
                                 __FILE__, __LINE__);
                    stack.push(name);
                    continue;
                }
            }

            if (Q_LIKELY(stack.pop(name))) {
                auto tdb = TSqlDatabase::database(name);
                if (Q_UNLIKELY(tdb.sqlDatabase().isOpen())) {
                    tSystemWarn("Gets a opend database: %s", qPrintable(tdb.connectionName()));
                    return tdb.sqlDatabase();
                } else {
                    if (Q_UNLIKELY(!tdb.sqlDatabase().open())) {
                        tError("Database open error. Invalid database settings, or maximum number of SQL connection exceeded.");
                        tSystemError("SQL database open error: %s", qPrintable(tdb.sqlDatabase().connectionName()));
                        stack.push(name);
                        return QSqlDatabase();
                    }

                    tSystemDebug("SQL database opened successfully (env:%s)",
                                 qPrintable(Tf::app()->databaseEnvironment()));
                    tSystemDebug("Gets database: %s", qPrintable(tdb.sqlDatabase().connectionName()));

                    // Executes setup-queries
                    if (! tdb.postOpenStatements().isEmpty()) {
                        TSqlQuery query(tdb.sqlDatabase());
                        for (QString st : tdb.postOpenStatements()) {
                            st = st.trimmed();
                            query.exec(st);
                        }
                    }
                    return tdb.sqlDatabase();
                }
            }
        }
    }
}


bool TSqlDatabasePool::setCertAuthSettings(TSqlDatabase &database, int databaseId, const QString &commonName)
{
    tSystemDebug("TSqlDatabasePool::setCertAuthSettings");
    // Initiates database
    auto settings = Tf::app()->sqlDatabaseSettings(databaseId);

    QString databaseName = settings.value("DatabaseName").toString().trimmed();
    if (databaseName.isEmpty()) {
        tError("Database name empty string");
        return false;
    }

    tSystemDebug("SQL driver name:%s  dbname:%s", qPrintable(database.sqlDatabase().driverName()), qPrintable(databaseName));
    if (database.dbmsType() == TSqlDatabase::SQLite) {
        if (!databaseName.contains(':')) {
            QFileInfo fi(databaseName);
            if (fi.isRelative()) {
                // For SQLite
                databaseName = Tf::app()->webRootPath() + databaseName;
            }
        }
    }
    database.sqlDatabase().setDatabaseName(databaseName);
    tSystemDebug("Database databaseName: %s", database.sqlDatabase().databaseName().toStdString().c_str());

    QString hostName = settings.value("HostName").toString().trimmed();
    if (!hostName.isEmpty()) {
        database.sqlDatabase().setHostName(hostName);
        tSystemDebug("Database hostName: %s", database.sqlDatabase().hostName().toStdString().c_str());
    }

    int port = settings.value("Port").toInt();
    if (port > 0) {
        tSystemDebug("Database Port: %d", port);
        database.sqlDatabase().setPort(port);
    }

    if (!commonName.isEmpty()) {
        database.sqlDatabase().setUserName(commonName);
        tSystemDebug("Database userName: %s", database.sqlDatabase().userName().toStdString().c_str());
    }

    QString connectOptions = QString("sslmode=verify-full;"
                             "sslrootcert=/home/jwc/CLionProjects/tf2blog/root.crt;"
                             "sslcert=%1/%2_crt.pem;"
                             "sslkey=%1/%2_key.pem").arg(settings.value("certDir").toString(), commonName);
    if (!connectOptions.isEmpty()) {
        database.sqlDatabase().setConnectOptions(connectOptions);
        tSystemDebug("Database connectOptions: %s", database.sqlDatabase().connectOptions().toStdString().c_str());
    }

    QStringList postOpenStatements = settings.value("PostOpenStatements").toString().trimmed().split(";", QString::SkipEmptyParts);
    tSystemDebug("Database postOpenStatements: %s", qPrintable(postOpenStatements.join(";")));
    if (!postOpenStatements.isEmpty()) {
        database.setPostOpenStatements(postOpenStatements);
    }

    bool enableUpsert = settings.value("EnableUpsert", false).toBool();
    tSystemDebug("Database enableUpsert: %d", enableUpsert);
    database.setUpsertEnabled(enableUpsert);

    auto *extension = TSqlDriverExtensionFactory::create(database.sqlDatabase().driverName(), database.sqlDatabase().driver());
    database.setDriverExtension(extension);

    return true;
}


bool TSqlDatabasePool::setDatabaseSettings(TSqlDatabase &database, int databaseId)
{
    // Initiates database
    auto settings = Tf::app()->sqlDatabaseSettings(databaseId);

    QString databaseName = settings.value("DatabaseName").toString().trimmed();
    if (databaseName.isEmpty()) {
        tError("Database name empty string");
        return false;
    }
    tSystemDebug("SQL driver name:%s  dbname:%s", qPrintable(database.sqlDatabase().driverName()), qPrintable(databaseName));
    if (database.dbmsType() == TSqlDatabase::SQLite) {
        if (!databaseName.contains(':')) {
            QFileInfo fi(databaseName);
            if (fi.isRelative()) {
                // For SQLite
                databaseName = Tf::app()->webRootPath() + databaseName;
            }
        }
    }
    database.sqlDatabase().setDatabaseName(databaseName);

    QString hostName = settings.value("HostName").toString().trimmed();
    tSystemDebug("Database HostName: %s", qPrintable(hostName));
    if (!hostName.isEmpty()) {
        database.sqlDatabase().setHostName(hostName);
    }

    int port = settings.value("Port").toInt();
    tSystemDebug("Database Port: %d", port);
    if (port > 0) {
        database.sqlDatabase().setPort(port);
    }

    QString userName = settings.value("UserName").toString().trimmed();
    tSystemDebug("Database UserName: %s", qPrintable(userName));
    if (!userName.isEmpty()) {
        database.sqlDatabase().setUserName(userName);
    }

    QString password = settings.value("Password").toString().trimmed();
    tSystemDebug("Database Password: %s", qPrintable(password));
    if (!password.isEmpty()) {
        database.sqlDatabase().setPassword(password);
    }

    QString connectOptions = settings.value("ConnectOptions").toString().trimmed();
    tSystemDebug("Database ConnectOptions: %s", qPrintable(connectOptions));
    if (!connectOptions.isEmpty()) {
        database.sqlDatabase().setConnectOptions(connectOptions);
    }

    QStringList postOpenStatements = settings.value("PostOpenStatements").toString().trimmed().split(";", QString::SkipEmptyParts);
    tSystemDebug("Database postOpenStatements: %s", qPrintable(postOpenStatements.join(";")));
    if (!postOpenStatements.isEmpty()) {
        database.setPostOpenStatements(postOpenStatements);
    }

    bool enableUpsert = settings.value("EnableUpsert", false).toBool();
    tSystemDebug("Database enableUpsert: %d", enableUpsert);
    database.setUpsertEnabled(enableUpsert);

    auto *extension = TSqlDriverExtensionFactory::create(database.sqlDatabase().driverName(), database.sqlDatabase().driver());
    database.setDriverExtension(extension);

    return true;
}


void TSqlDatabasePool::pool(QSqlDatabase &database, bool forceClose)
{
    if (database.isValid()) {
        int databaseId = getDatabaseId(database);
        tSystemDebug("Pooled datbase id: %d", databaseId);

        if (Q_LIKELY(databaseId >= 0 && databaseId < Tf::app()->sqlDatabaseSettingsCount())) {
            auto commonName = Tf::app()->sqlDatabaseSettings(databaseId).value("commonName").toString();
            tSystemDebug("Pooling commonName: %s", commonName.toStdString().c_str());
            if (!commonName.isNull()) {
                tSystemDebug("--- Ooooweee");
            }
            else {
            if (forceClose) {
                tSystemWarn("Force close database: %s", qPrintable(database.connectionName()));
                closeDatabase(database);
            } else {
                // pool
                cachedDatabase[databaseId].push(database.connectionName());
                lastCachedTime[databaseId].store((uint)std::time(nullptr));
                tSystemDebug("Pooled database: %s", qPrintable(database.connectionName()));
                }
            }
        } else {
            tSystemError("Pooled invalid database  [%s:%d]", __FILE__, __LINE__);
        }
    }
    database = QSqlDatabase();  // Sets an invalid object
}


void TSqlDatabasePool::timerEvent(QTimerEvent *event)
{
//    tSystemDebug("TSqlDatabasePool::timerEvent");
    if (event->timerId() == timer.timerId()) {
        QString name;

        // Closes extra-connection
        for (int i = 0; i < Tf::app()->sqlDatabaseSettingsCount(); ++i) {
            auto commonName = Tf::app()->sqlDatabaseSettings(i).value("commonName").toString();
//            tSystemDebug("Pooling commonName: %s", commonName.toStdString().c_str());
            if (!commonName.isNull()) {
//                tSystemDebug("--- timerEvent");
                userDbCache->expire();
            }
            else {
            auto &cache = cachedDatabase[i];
            if (cache.count() == 0) {
                continue;
            }

                while (lastCachedTime[i].load() < (uint) std::time(nullptr) - 30 && cache.pop(name)) {
                QSqlDatabase db = TSqlDatabase::database(name).sqlDatabase();
                closeDatabase(db);
                }
            }
        }
    } else {
        QObject::timerEvent(event);
    }
}


void TSqlDatabasePool::closeDatabase(QSqlDatabase &database)
{
    int id = getDatabaseId(database);
    QString name = database.connectionName();
    database.close();
    tSystemDebug("Closed database connection, name: %s", qPrintable(name));
    availableNames[id].push(name);
}


int TSqlDatabasePool::getDatabaseId(const QSqlDatabase &database)
{
    bool ok;
    int id = database.connectionName().mid(3,2).toInt(&ok);

    if (Q_LIKELY(ok && id >= 0)) {
        return id;
    }
    return -1;
}
