namespace cpp graphps

typedef i64 ValueDtype
typedef i64 MsgDtype
typedef i32 DegreeDtype

struct VertexData {
  1: ValueDtype value,
  2: MsgDtype msg,
  3: optional DegreeDtype indegree,
}

service VertexUpdate {
   void ping()
}
