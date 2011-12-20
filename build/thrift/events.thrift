namespace java com.lockerz.event

enum EventType {
  LOGIN,
  METHOD_CALL,
  HTTP_REQUEST
}

struct Login {
  1: required i32 userId;
  2: required i64 eventTime;
  3: optional EventType eventType = EventType.LOGIN;
}

struct MethodCall {
  1: required string className;
  2: required string methodName;
  3: optional i64 runTimeMs = -1;
  4: optional EventType eventType = EventType.METHOD_CALL;
}
