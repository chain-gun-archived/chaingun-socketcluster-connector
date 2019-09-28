import { GunGraphWireConnector } from "@notabug/chaingun"
import socketCluster from "socketcluster-client"
import { SCChannel, SCChannelOptions } from "sc-channel"
import { sign } from "@notabug/gun-sear"

export class SocketClusterGraphConnector extends GunGraphWireConnector {
  opts: socketCluster.SCClientSocket.ClientOptions | undefined
  socket?: socketCluster.SCClientSocket
  msgChannel?: SCChannel
  getsChannel?: SCChannel
  putsChannel?: SCChannel

  constructor(
    opts: socketCluster.SCClientSocket.ClientOptions | undefined,
    name = "SocketClusterGraphConnector"
  ) {
    super(name)
    this.outputQueue.completed.on(this._onOutputProcessed.bind(this))
    this.opts = opts
    this._connectToCluster()
  }

  get({
    soul,
    cb
  }: {
    soul: string
    msgId?: string
    key?: string
    cb?: Function
  }) {
    const cbWrap = (msg: any) => {
      this.ingest([msg])
      if (cb) cb(msg)
    }

    const channel = this.subscribeToChannel(`gun/nodes/${soul}`, cbWrap)
    return () => {
      channel.unsubscribe()
    }
  }

  authenticate(pub: string, priv: string) {
    const doAuth = () => {
      const id = this.socket!.id
      const timestamp = new Date().getTime()
      const challenge = `${id}/${timestamp}`
      return sign(challenge, { pub, priv }, { raw: true }).then(
        proof =>
          new Promise((ok, fail) => {
            this.socket!.emit(
              "login",
              {
                pub,
                proof
              },
              (err: any, rejection: any) => {
                if (err || rejection) {
                  fail(err || rejection)
                } else {
                  ok()
                }
              }
            )
          })
      )
    }

    return this.waitForConnection().then(() => {
      doAuth()
      this.socket!.on("connect", doAuth)
    })
  }

  subscribeToChannel(
    channelName: string,
    cb?: Function,
    opts?: SCChannelOptions
  ) {
    const channel = this.socket!.subscribe(channelName, opts)
    channel.on("subscribe", () => {
      channel.watch(msg => {
        this.ingest([msg])
        if (cb) cb(msg)
      })
    })
    return channel
  }

  publishToChannel(channel: string, msg: any) {
    this.socket!.publish(channel, msg)
  }

  protected _connectToCluster() {
    this.socket = socketCluster.create(this.opts)
    this.socket.on("connect", () => {
      this.events.connection.trigger(true)
    })
    this.socket.on("error", err => {
      console.error("SC Connection Error", err.stack, err)
    })
  }

  private _onOutputProcessed(msg: any) {
    if (msg && this.socket) {
      const replyTo = msg["@"]
      if (replyTo) {
        this.publishToChannel(`gun/@${replyTo}`, msg)
      } else {
        if ("get" in msg) {
          this.publishToChannel("gun/get", msg)
        } else if ("put" in msg) {
          this.publishToChannel("gun/put", msg)
        }
      }
    }
  }
}
