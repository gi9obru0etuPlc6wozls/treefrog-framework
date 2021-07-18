#pragma once
#include <QSqlDatabase>
#include <TGlobal>
#include "tsqldatabase.h"

/*!
  \class Transaction
  \brief The Transaction class provides a transaction of database.
*/

class T_CORE_EXPORT TSqlTransaction {
public:
    TSqlTransaction();
    TSqlTransaction(const TSqlTransaction &other) = default;
    ~TSqlTransaction();

    TSqlTransaction &operator=(const TSqlTransaction &) = default;
    TSqlDatabase &tdatabase() { return _tdatabase; }
    bool begin();
    bool commit();
    bool rollback();
    bool isActive() const { return _active; }
    void setEnabled(bool enable);
    void setDisabled(bool disable);
    void setCommonName(const QString &commonName);
    const QString &commonName() const;

private:
    TSqlDatabase _tdatabase;
    bool _enabled {true};
    bool _active {false};
    QString _connectionName;
    QString _commonName;
};


inline void TSqlTransaction::setEnabled(bool enable)
{
    _enabled = enable;
}


inline void TSqlTransaction::setDisabled(bool disable)
{
    _enabled = !disable;
}

