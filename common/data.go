package common

type Request struct {
	A, B int
}

type Response struct {
	R int
}

type RequestRPC struct {
	funType    string
	filename   string
	inProgress bool
}
