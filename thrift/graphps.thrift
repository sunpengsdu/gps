namespace cpp graphps

typedef i32 VidDtype
typedef double VvalueDtype
typedef double VmsgDtype
typedef i32 VdegDtype

struct VertexData {
  1: VvalueDtype value,
  2: VmsgDtype msg,
  3: bool state,
  4: optional VdegDtype outdegree,
}

service VertexUpdate {
   i32 ping(1: i32 id)
}
