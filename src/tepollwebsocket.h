#ifndef TEPOLLWEBSOCKET_H
#define TEPOLLWEBSOCKET_H

#include <TGlobal>
#include <THttpRequestHeader>
#include <THttpResponseHeader>
#include "tepollsocket.h"
#include "tabstractwebsocket.h"

class QHostAddress;
class TWebSocketFrame;
class TSession;


class T_CORE_EXPORT TEpollWebSocket : public TEpollSocket, public TAbstractWebSocket
{
    Q_OBJECT
public:
    virtual ~TEpollWebSocket();

    bool isTextRequest() const;
    bool isBinaryRequest() const;
    QByteArray readBinaryRequest();
    virtual bool canReadRequest();
    virtual void startWorker();
    void startWorkerForOpening(const TSession &session);

    static void sendText(const QByteArray &socketUuid, const QString &message);
    static void sendBinary(const QByteArray &socketUuid, const QByteArray &data);
    static void sendPing(const QByteArray &socketUuid);
    static void sendPong(const QByteArray &socketUuid);
    static void disconnect(const QByteArray &socketUuid);

public slots:
    void releaseWorker();

protected:
    virtual void *getRecvBuffer(int size);
    virtual bool seekRecvBuffer(int pos);
    virtual QList<TWebSocketFrame> &websocketFrames() { return frames; }
    void clear();

private:
    THttpRequestHeader reqHeader;
    QByteArray recvBuffer;
    QList<TWebSocketFrame> frames;

    TEpollWebSocket(int socketDescriptor, const QHostAddress &address, const THttpRequestHeader &header);

    friend class TEpoll;
    Q_DISABLE_COPY(TEpollWebSocket)
};

#endif // TEPOLLWEBSOCKET_H
