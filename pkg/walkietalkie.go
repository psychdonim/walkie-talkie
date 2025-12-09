package pkg

import (
	"encoding/binary"
	"io"
	"net"
)

const oneMessageBorder = 1 << 20 // 1 MB
const chunkSize = 1<<16 - 1

type WTChunksListener func(<-chan []byte, io.Writer)

type WalkieTalkie struct {
	listenAddr net.Addr
}

func NewWalkieTalkie(listenAddr net.Addr) *WalkieTalkie {
	return &WalkieTalkie{
		listenAddr: listenAddr,
	}
}

func (wt *WalkieTalkie) ListenWithCallback(callback WTChunksListener) error {
	listener, err := net.Listen(wt.listenAddr.Network(), wt.listenAddr.String())
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		chunksDrain := make(chan []byte, 4)
		go callback(chunksDrain, conn)
		go wt.ListenFrames(conn, chunksDrain)
	}
}

func (wt *WalkieTalkie) ListenFrames(conn net.Conn, chunksDrain chan<- []byte) {
	defer conn.Close()
	defer close(chunksDrain)

	header := make([]byte, 8)
	if _, err := io.ReadFull(conn, header); err != nil {
		return
	}
	messageLength := binary.BigEndian.Uint64(header)

	chunk := make([]byte, chunkSize)
	var offset uint64 = 0
	for offset < messageLength {
		n, err := conn.Read(chunk)
		if err != nil {
			return
		}

		chunksDrain <- chunk[:n]
		offset += uint64(n)
	}
}

func (wt *WalkieTalkie) Send(messageReader io.Reader, messageLength uint64) error {
	conn, err := net.Dial(wt.listenAddr.Network(), wt.listenAddr.String())
	if err != nil {
		return err
	}
	defer conn.Close()

	if messageLength < oneMessageBorder {
		oneshot := make([]byte, messageLength)
		if _, err := messageReader.Read(oneshot); err != nil {
			return err
		}
		_, err := conn.Write(oneshot)
		return err
	}

	var offset uint64 = 0

	startFrame := make([]byte, 8)
	binary.BigEndian.PutUint64(startFrame, messageLength)
	if _, err := conn.Write(startFrame); err != nil {
		return err
	}

	chunk := make([]byte, chunkSize)
	for offset < messageLength {
		n, err := messageReader.Read(chunk)
		if err != nil {
			return err
		}

		frameLen := 2 + n
		frame := make([]byte, frameLen)
		binary.BigEndian.PutUint16(frame[:2], uint16(n))
		copy(frame[2:], chunk)

		if _, err := conn.Write(frame); err != nil {
			return err
		}
		offset += uint64(n)
	}

	return nil
}
