#pragma once
#include "tatomic.h"
#include "tstack.h"
#include <QBasicTimer>
#include <QDateTime>
#include <QMap>
#include <QObject>
#include <QSqlDatabase>
#include <QString>
#include <QVector>
#include <TGlobal>
#include "tatomic.h"
#include "tstack.h"
#include <unordered_map>
#include <list>
#include <TSystemGlobal>
#include "tsqldatabase.h"

class TSqlDatabase;

typedef std::pair<QString, std::time_t> key_value_pair_t;
typedef std::list<key_value_pair_t>::iterator list_iterator_t;
typedef std::unordered_map<QString, list_iterator_t> item_map_t;
typedef std::list<key_value_pair_t> item_list_t;
typedef int max_size_t;

/*
namespace std {
    template<>
    struct hash<QString> {
        std::size_t operator()(const QString &s) const noexcept {
            return (size_t) qHash(s);
        }
    };
}
*/

class TLruCache {

public:
    explicit TLruCache(max_size_t max_size) : _max_size(max_size) {

    }

    void cache(const QString &key) {
        auto it = _cache_items_map.find(key);
        _cache_items_list.push_front(key_value_pair_t(key, std::time(nullptr)));
        if (it != _cache_items_map.end()) {
            _cache_items_list.erase(it->second);
            _cache_items_map.erase(it);
        }
        _cache_items_map[key] = _cache_items_list.begin();

        max_size_t map_size = _cache_items_map.size();

        if (map_size > _max_size) {
            auto last = _cache_items_list.end();
            last--;
            tSystemDebug("Max size purge: %s", last->first.toStdString().c_str());
            _cache_items_map.erase(last->first);
            _cache_items_list.pop_back();
            if (TSqlDatabase::contains(last->first)) {
                tSystemDebug("Contains: %s", last->first.toStdString().c_str());
                QSqlDatabase db = TSqlDatabase::database(last->first).sqlDatabase();
                db.close();
                TSqlDatabase::removeDatabase(last->first);
            }
        }
    }

    std::time_t get(const QString &key) {
        auto it = _cache_items_map.find(key);
        if (it == _cache_items_map.end()) {
            throw std::range_error("There is no such key in cache");;
        }
        _cache_items_list.splice(_cache_items_list.begin(), _cache_items_list, it->second);
        return it->second->second;
    }

    void print() {
        tSystemDebug("_cache_items_map:");
        for (item_map_t::iterator i = _cache_items_map.begin(); i != _cache_items_map.end(); ++i) {
            tSystemDebug("key: %s", i->first.toStdString().c_str());
            tSystemDebug("first: %s", i->second->first.toStdString().c_str());
            tSystemDebug("second: %ld", i->second->second);
        }
        tSystemDebug("_cache_items_list:");
        for (item_list_t::iterator i = _cache_items_list.begin(); i != _cache_items_list.end(); ++i) {
            tSystemDebug("first: %s", i->first.toStdString().c_str());
            tSystemDebug("second: %ld", i->second);
        }
    }

    void expire() {
        //tSystemDebug("expire:");
        for (list_iterator_t i = _cache_items_list.begin(); i != _cache_items_list.end();) {
            auto age = std::time(nullptr) - i->second;
            //tSystemDebug("first: %s age: %ld", i->first.toStdString().c_str(), age);
            if (age > 60) {
                tSystemDebug("Has expired: %s", i->first.toStdString().c_str());
                //tSystemDebug("first: %s", i->first.toStdString().c_str());
                //tSystemDebug("second: %ld", i->second);
                tSystemDebug("time: %ld", age);
                if (TSqlDatabase::contains(i->first)) {
                    tSystemDebug("Closing db: %s", i->first.toStdString().c_str());
                    QSqlDatabase db = TSqlDatabase::database(i->first).sqlDatabase();
                    db.close();
                    tSystemDebug("Removing db: %s", i->first.toStdString().c_str());
                    TSqlDatabase::removeDatabase(i->first);
                }
                tSystemDebug("Erasing from cache: %s", i->first.toStdString().c_str());
                _cache_items_map.erase(i->first);
                i = _cache_items_list.erase(i);
            }
            else {
                ++i;
            }
        }
    }

    bool exists(const QString &key) const {
        return _cache_items_map.find(key) != _cache_items_map.end();
    }

    size_t size() const {
        return _cache_items_map.size();
    }

private:
    item_map_t _cache_items_map;
    item_list_t _cache_items_list;
    max_size_t _max_size{0};
};


class T_CORE_EXPORT TSqlDatabasePool : public QObject {
    Q_OBJECT
public:
    ~TSqlDatabasePool();

    QSqlDatabase x_database(int databaseId = 0, const QString &commonName = "");

    void pool(QSqlDatabase &database, bool forceClose = false);

    static TSqlDatabasePool *instance();

    static bool setDatabaseSettings(TSqlDatabase &database, int databaseId);

    static bool setCertAuthSettings(TSqlDatabase &database, int databaseId, const QString &commonName);

    static int getDatabaseId(const QSqlDatabase &database);

protected:
    void init();

    void timerEvent(QTimerEvent *event);

    void closeDatabase(QSqlDatabase &database);

private:
    TSqlDatabasePool();

    TLruCache *userDbCache{nullptr};
    TStack<QString> *cachedDatabase {nullptr};
    TAtomic<uint> *lastCachedTime {nullptr};
    TStack<QString> *availableNames {nullptr};
    max_size_t maxConnects{0};
    QBasicTimer timer;
    QMutex mutex;

    T_DISABLE_COPY(TSqlDatabasePool)

    T_DISABLE_MOVE(TSqlDatabasePool)
};

