/* Copyright (c) 2010-2019, AOYAMA Kazuharu
 * All rights reserved.
 *
 * This software may be used and distributed according to the terms of
 * the New BSD License, which is incorporated herein by reference.
 */

#include "sqlobjgenerator.h"
#include "filewriter.h"
#include "global.h"
#include "tableschema.h"

constexpr auto SQLOBJECT_HEADER_TEMPLATE = "#ifndef %1OBJECT_H\n"
                                           "#define %1OBJECT_H\n"
                                           "\n"
                                           "#include <TSqlObject>\n"
                                           "#include <QSharedData>\n"
                                           "#include <QUuid>\n"
                                           "\n\n"
                                           "class T_MODEL_EXPORT %2Object : public TSqlObject, public QSharedData\n"
                                           "{\n"
                                           "public:\n";

constexpr auto SQLOBJECT_PROPERTY_TEMPLATE = "    Q_PROPERTY(%1 %2 READ get%2 WRITE set%2)\n"
                                             "    T_DEFINE_PROPERTY(%1, %2)\n";

constexpr auto SQLOBJECT_FOOTER_TEMPLATE = "};\n"
                                           "\n"
                                           "#endif // %1OBJECT_H\n";


static bool isNumericType(const QString &typeName)
{
    switch (QMetaType::type(typeName.toLatin1())) {
    case QMetaType::Int:
    case QMetaType::UInt:
    case QMetaType::Long:
    case QMetaType::LongLong:
    case QMetaType::Short:
    case QMetaType::Char:
    case QMetaType::UChar:
    case QMetaType::ULong:
    case QMetaType::ULongLong:
    case QMetaType::UShort:
    case QMetaType::Double:
    case QMetaType::Float:
    case QMetaType::SChar:
        return true;
    default:
        return false;
    }
}


inline bool isBoolType(const QString &typeName)
{
    return (QMetaType::type(typeName.toLatin1()) == QMetaType::Bool);
}


SqlObjGenerator::SqlObjGenerator(const QString &model, const QString &table) :
    tableSch(new TableSchema(table))
{
    modelName = (!model.isEmpty()) ? model : fieldNameToEnumName(table);
}


SqlObjGenerator::~SqlObjGenerator()
{
    delete tableSch;
}


QString SqlObjGenerator::generate(const QString &dstDir)
{
    QList<QPair<QString, QString>> fieldList = tableSch->getFieldList();
    if (fieldList.isEmpty()) {
        qCritical("table not found, %s", qPrintable(tableSch->tableName()));
        return QString();
    }

    QString output;

    // Header part
    output += QString(SQLOBJECT_HEADER_TEMPLATE).arg(modelName.toUpper(), modelName);
    QListIterator<QPair<QString, QString>> it(fieldList);
    while (it.hasNext()) {
        const QPair<QString, QString> &p = it.next();
        if (isNumericType(p.second)) {
            output += QString("    %1 %2 {0};\n").arg(p.second, p.first);
        } else if (isBoolType(p.second)) {
            output += QString("    %1 %2 {false};\n").arg(p.second, p.first);
        } else {
            output += QString("    %1 %2;\n").arg(p.second, p.first);
        }
    }

    // enum part
    output += QLatin1String("\n    enum PropertyIndex {\n");
    it.toFront();
    const QPair<QString, QString> &p = it.next();
    output += QString("        %1 = 0,\n").arg(fieldNameToEnumName(p.first));
    while (it.hasNext()) {
        const QPair<QString, QString> &p = it.next();
        output += QString("        %1,\n").arg(fieldNameToEnumName(p.first));
    }
    output += QLatin1String("    };\n\n");

    // Property Index Map
    output += QLatin1String("\n    inline static const QMap<QString, int> &propertyIndexMap() {");
    output += QLatin1String("\n        static const QMap<QString, int> propertyIndexMap{\n");
    it.toFront();
    while (it.hasNext()) {
        const QPair<QString, QString> &p = it.next();
        output += QString("            { \"%1\",").arg(fieldNameToVariableName(p.first));
        output += QString(" %1Object::PropertyIndex::").arg(modelName);
        output += QString("%1 },\n").arg(fieldNameToEnumName(p.first));
    }
    output += QLatin1String("        };\n");
    output += QLatin1String("        return propertyIndexMap;\n");
    output += QLatin1String("    }\n\n");


    output += QString("    QList<int> *omitColumns() override {\n");
    output += QString("\n");
    output += QString("        static QList<int> *list = nullptr;\n");
    output += QString("\n");
    output += QString("        if (list == nullptr) {\n");
    output += QString("            list = new QList<int>{\n");
    output += QString("                %1Object::PropertyIndex::Id,\n").arg(modelName);
    output += QString("                %1Object::PropertyIndex::CreatedAt,\n").arg(modelName);
    output += QString("                %1Object::PropertyIndex::UpdatedAt,\n").arg(modelName);
    output += QString("                %1Object::PropertyIndex::CreatedById,\n").arg(modelName);
    output += QString("                %1Object::PropertyIndex::UpdatedById\n").arg(modelName);
    output += QString("            };\n");
    output += QString("        }\n");
    output += QString("        return list;\n");
    output += QString("    }\n\n");

    // primaryKeyIndex() method
    output += QLatin1String("    int primaryKeyIndex() const override { return ");
    QString pkName = tableSch->primaryKeyFieldName();
    if (pkName.isEmpty()) {
        output += QLatin1String("-1; }\n");
    } else {
        output += fieldNameToEnumName(pkName);
        output += QLatin1String("; }\n");
    }

    // auto-value field, for example auto-increment value
    output += QLatin1String("    int autoValueIndex() const override { return ");
    QString autoValue = tableSch->autoValueFieldName();
    if (autoValue.isEmpty()) {
        output += QLatin1String("-1; }\n");
    } else {
        output += fieldNameToEnumName(autoValue);
        output += QLatin1String("; }\n");
    }

    // tableName() method
    output += QLatin1String("    QString tableName() const override { return QStringLiteral(\"");
    output += tableSch->tableName();
    output += QLatin1String("\"); }\n\n");

    // Property macros part
    output += QLatin1String("private:    /*** Don't modify below this line ***/\n    Q_OBJECT\n");
    it.toFront();
    while (it.hasNext()) {
        const QPair<QString, QString> &p = it.next();
        output += QString(SQLOBJECT_PROPERTY_TEMPLATE).arg(p.second, p.first);
    }

    // Footer part
    output += QString(SQLOBJECT_FOOTER_TEMPLATE).arg(modelName.toUpper());

    // Writes to a file
    QDir dst = QDir(dstDir).filePath("sqlobjects");
    FileWriter fw(dst.filePath(modelName.toLower() + "object.h"));
    fw.write(output, false);
    return QLatin1String("sqlobjects/") + fw.fileName();
}


QList<QPair<QString, QVariant::Type>> SqlObjGenerator::fieldList() const
{
    return tableSch->getFieldTypeList();
}


int SqlObjGenerator::primaryKeyIndex() const
{
    return tableSch->primaryKeyIndex();
}


int SqlObjGenerator::autoValueIndex() const
{
    return tableSch->autoValueIndex();
}


int SqlObjGenerator::lockRevisionIndex() const
{
    return tableSch->lockRevisionIndex();
}
