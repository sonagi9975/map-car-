import json
from datetime import datetime
from typing import Dict, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ==========================================
# 1. Database Model Setup
# ==========================================
DATABASE_URL = "sqlite:///./shared_vehicle.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class VehicleState(Base):
    """차량의 현재 실시간 상태 (공유 상태 관리)"""
    __tablename__ = "vehicle_states"
    
    vehicle_id = Column(String, primary_key=True, index=True)
    is_driving = Column(Boolean, default=False)
    driving_started_by = Column(String, nullable=True)
    driving_start_time = Column(DateTime, nullable=True)
    current_drive_id = Column(Integer, nullable=True)
    
    is_parked = Column(Boolean, default=False)
    parked_by = Column(String, nullable=True)
    parking_start_time = Column(DateTime, nullable=True)
    current_parking_id = Column(Integer, nullable=True)

class DriveLog(Base):
    """주행 기록 (수정 가능)"""
    __tablename__ = "drive_logs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    vehicle_id = Column(String, index=True)
    driver_name = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime, nullable=True)
    ended_by = Column(String, nullable=True)
    distance_km = Column(Integer, default=0)
    memo = Column(String, nullable=True)

class ParkingLog(Base):
    """주차 기록 (수정 가능)"""
    __tablename__ = "parking_logs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    vehicle_id = Column(String, index=True)
    parked_by = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime, nullable=True)
    unparked_by = Column(String, nullable=True)
    location = Column(String, nullable=True)
    memo = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==========================================
# 2. Real-Time WebSocket Connection Manager
# ==========================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, vehicle_id: str, websocket: WebSocket):
        await websocket.accept()
        if vehicle_id not in self.active_connections:
            self.active_connections[vehicle_id] = []
        self.active_connections[vehicle_id].append(websocket)

    def disconnect(self, vehicle_id: str, websocket: WebSocket):
        if vehicle_id in self.active_connections:
            self.active_connections[vehicle_id].remove(websocket)

    async def broadcast(self, vehicle_id: str, message: dict):
        if vehicle_id in self.active_connections:
            for connection in self.active_connections[vehicle_id]:
                await connection.send_text(json.dumps(message, default=str))

manager = ConnectionManager()
app = FastAPI(title="Shared Vehicle Operations API")

class LogUpdateSchema(BaseModel):
    location: Optional[str] = None
    distance_km: Optional[int] = None
    memo: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

def get_vehicle_state(db: Session, vehicle_id: str) -> VehicleState:
    state = db.query(VehicleState).filter(VehicleState.vehicle_id == vehicle_id).first()
    if not state:
        state = VehicleState(vehicle_id=vehicle_id)
        db.add(state)
        db.commit()
        db.refresh(state)
    return state

# ==========================================
# 3. API Routes
# ==========================================

@app.post("/vehicle/{vehicle_id}/drive/start")
async def start_drive(vehicle_id: str, user_name: str, db: Session = Depends(get_db)):
    state = get_vehicle_state(db, vehicle_id)
    if state.is_driving:
        raise HTTPException(status_code=400, detail="이미 다른 사용자에 의해 주행 중입니다.")
    if state.is_parked:
        raise HTTPException(status_code=400, detail="주차를 먼저 해제해 주세요.")

    now = datetime.now()
    new_log = DriveLog(vehicle_id=vehicle_id, driver_name=user_name, start_time=now)
    db.add(new_log)
    db.flush()
    
    state.is_driving = True
    state.driving_started_by = user_name
    state.driving_start_time = now
    state.current_drive_id = new_log.id
    db.commit()

    payload = {"event": "DRIVE_STARTED", "vehicle_id": vehicle_id, "user_name": user_name, "start_time": now.isoformat(), "drive_id": new_log.id}
    await manager.broadcast(vehicle_id, payload)
    return payload

@app.post("/vehicle/{vehicle_id}/drive/end")
async def end_drive(vehicle_id: str, user_name: str, distance_km: int = 0, db: Session = Depends(get_db)):
    state = get_vehicle_state(db, vehicle_id)
    if not state.is_driving:
        raise HTTPException(status_code=400, detail="현재 주행 중인 상태가 아닙니다.")

    now = datetime.now()
    log = db.query(DriveLog).filter(DriveLog.id == state.current_drive_id).first()
    if log:
        log.end_time = now
        log.ended_by = user_name
        log.distance_km = distance_km

    state.is_driving = False
    state.driving_started_by = None
    state.driving_start_time = None
    state.current_drive_id = None
    db.commit()

    payload = {"event": "DRIVE_ENDED", "vehicle_id": vehicle_id, "ended_by": user_name, "end_time": now.isoformat(), "distance_km": distance_km}
    await manager.broadcast(vehicle_id, payload)
    return payload

@app.post("/vehicle/{vehicle_id}/parking/start")
async def start_parking(vehicle_id: str, user_name: str, location: str = "", db: Session = Depends(get_db)):
    state = get_vehicle_state(db, vehicle_id)
    if state.is_parked:
        raise HTTPException(status_code=400, detail="이미 주차되어 있는 상태입니다.")
    if state.is_driving:
        raise HTTPException(status_code=400, detail="주행을 먼저 종료해 주세요.")

    now = datetime.now()
    new_log = ParkingLog(vehicle_id=vehicle_id, parked_by=user_name, start_time=now, location=location)
    db.add(new_log)
    db.flush()

    state.is_parked = True
    state.parked_by = user_name
    state.parking_start_time = now
    state.current_parking_id = new_log.id
    db.commit()

    payload = {"event": "PARKING_STARTED", "vehicle_id": vehicle_id, "parked_by": user_name, "start_time": now.isoformat(), "location": location, "parking_id": new_log.id}
    await manager.broadcast(vehicle_id, payload)
    return payload

@app.post("/vehicle/{vehicle_id}/parking/end")
async def end_parking(vehicle_id: str, user_name: str, db: Session = Depends(get_db)):
    state = get_vehicle_state(db, vehicle_id)
    if not state.is_parked:
        raise HTTPException(status_code=400, detail="현재 주차 상태가 아닙니다.")

    now = datetime.now()
    log = db.query(ParkingLog).filter(ParkingLog.id == state.current_parking_id).first()
    if log:
        log.end_time = now
        log.unparked_by = user_name

    state.is_parked = False
    state.parked_by = None
    state.parking_start_time = None
    state.current_parking_id = None
    db.commit()

    payload = {"event": "PARKING_ENDED", "vehicle_id": vehicle_id, "unparked_by": user_name, "end_time": now.isoformat()}
    await manager.broadcast(vehicle_id, payload)
    return payload

@app.patch("/logs/drive/{log_id}")
async def update_drive_log(log_id: int, update_data: LogUpdateSchema, db: Session = Depends(get_db)):
    log = db.query(DriveLog).filter(DriveLog.id == log_id).first()
    if not log:
        raise HTTPException(status_code=404, detail="해당 주행 기록을 찾을 수 없습니다.")

    if update_data.distance_km is not None: log.distance_km = update_data.distance_km
    if update_data.memo is not None: log.memo = update_data.memo
    if update_data.start_time: log.start_time = update_data.start_time
    if update_data.end_time: log.end_time = update_data.end_time

    db.commit()
    db.refresh(log)
    await manager.broadcast(log.vehicle_id, {"event": "LOG_UPDATED", "type": "DRIVE", "log_id": log_id})
    return {"status": "success", "data": log}

@app.patch("/logs/parking/{log_id}")
async def update_parking_log(log_id: int, update_data: LogUpdateSchema, db: Session = Depends(get_db)):
    log = db.query(ParkingLog).filter(ParkingLog.id == log_id).first()
    if not log:
        raise HTTPException(status_code=404, detail="해당 주차 기록을 찾을 수 없습니다.")

    if update_data.location is not None: log.location = update_data.location
    if update_data.memo is not None: log.memo = update_data.memo
    if update_data.start_time: log.start_time = update_data.start_time
    if update_data.end_time: log.end_time = update_data.end_time

    db.commit()
    db.refresh(log)
    await manager.broadcast(log.vehicle_id, {"event": "LOG_UPDATED", "type": "PARKING", "log_id": log_id})
    return {"status": "success", "data": log}

@app.websocket("/ws/{vehicle_id}")
async def websocket_endpoint(websocket: WebSocket, vehicle_id: str):
    await manager.connect(vehicle_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(vehicle_id, websocket)
        import json
from datetime import datetime
from typing import Dict, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ==========================================
# 1. DB 설정 (SQLite)
# ==========================================
DATABASE_URL = "sqlite:///./shared_team_vehicle.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class TeamState(Base):
    """팀별 실시간 상태 (A팀, B팀 각각 개별 관리)"""
    __tablename__ = "team_states"
    
    team_id = Column(String, primary_key=True, index=True) # 예: "A팀", "B팀"
    is_driving = Column(Boolean, default=False)
    driving_started_by = Column(String, nullable=True)
    current_drive_id = Column(Integer, nullable=True)
    
    is_parked = Column(Boolean, default=False)
    parked_by = Column(String, nullable=True)
    current_parking_id = Column(Integer, nullable=True)

class DriveLog(Base):
    """전체 주행 기록 (누구나 조회 및 수정 가능)"""
    __tablename__ = "drive_logs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    team_id = Column(String, index=True)       # 기록한 팀
    driver_name = Column(String)               # 시작한 사람
    ended_by = Column(String, nullable=True)   # 종료한 사람
    start_time = Column(DateTime)
    end_time = Column(DateTime, nullable=True)
    distance_km = Column(Integer, default=0)   # [수정 가능]
    memo = Column(String, nullable=True)        # [수정 가능]

class ParkingLog(Base):
    """전체 주차 기록 (누구나 조회 및 수정 가능)"""
    __tablename__ = "parking_logs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    team_id = Column(String, index=True)       # 기록한 팀
    parked_by = Column(String)                 # 시작한 사람
    unparked_by = Column(String, nullable=True)# 종료한 사람
    start_time = Column(DateTime)
    end_time = Column(DateTime, nullable=True)
    location = Column(String, nullable=True)   # [수정 가능]
    memo = Column(String, nullable=True)        # [수정 가능]

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==========================================
# 2. 팀별 실시간 소켓 매니저
# ==========================================
class ConnectionManager:
    def __init__(self):
        # team_id 별로 접속된 웹소켓 관리
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, team_id: str, websocket: WebSocket):
        await websocket.accept()
        if team_id not in self.active_connections:
            self.active_connections[team_id] = []
        self.active_connections[team_id].append(websocket)

    def disconnect(self, team_id: str, websocket: WebSocket):
        if team_id in self.active_connections:
            self.active_connections[team_id].remove(websocket)

    async def broadcast_to_team(self, team_id: str, message: dict):
        """같은 팀원들에게만 실시간 상태 전달 (A팀 1번이 켜면 A팀 2번도 켜짐)"""
        if team_id in self.active_connections:
            for connection in self.active_connections[team_id]:
                await connection.send_text(json.dumps(message, default=str))

manager = ConnectionManager()
app = FastAPI(title="Team Shared Vehicle System")

class LogUpdateSchema(BaseModel):
    location: Optional[str] = None
    distance_km: Optional[int] = None
    memo: Optional[str] = None

def get_team_state(db: Session, team_id: str) -> TeamState:
    state = db.query(TeamState).filter(TeamState.team_id == team_id).first()
    if not state:
        state = TeamState(team_id=team_id)
        db.add(state)
        db.commit()
        db.refresh(state)
    return state

# ==========================================
# 3. 팀별 주행 / 주차 스위치 API
# ==========================================

@app.post("/team/{team_id}/drive/start")
async def start_drive(team_id: str, user_name: str, db: Session = Depends(get_db)):
    """A팀 1번이 주행 시작"""
    state = get_team_state(db, team_id)
    if state.is_driving:
        raise HTTPException(status_code=400, detail="이미 팀원이 주행 중입니다.")
    
    now = datetime.now()
    log = DriveLog(team_id=team_id, driver_name=user_name, start_time=now)
    db.add(log)
    db.flush()
    
    state.is_driving = True
    state.driving_started_by = user_name
    state.current_drive_id = log.id
    db.commit()

    # 같은 팀원들에게만 상태 브로드캐스트
    payload = {"event": "DRIVE_STARTED", "team_id": team_id, "user_name": user_name, "log_id": log.id}
    await manager.broadcast_to_team(team_id, payload)
    return payload

@app.post("/team/{team_id}/drive/end")
async def end_drive(team_id: str, user_name: str, distance_km: int = 0, db: Session = Depends(get_db)):
    """A팀 2번이 주행 종료 (교차 끄기)"""
    state = get_team_state(db, team_id)
    if not state.is_driving:
        raise HTTPException(status_code=400, detail="주행 중이 아닙니다.")

    now = datetime.now()
    log = db.query(DriveLog).filter(DriveLog.id == state.current_drive_id).first()
    if log:
        log.end_time = now
        log.ended_by = user_name
        log.distance_km = distance_km

    state.is_driving = False
    state.driving_started_by = None
    state.current_drive_id = None
    db.commit()

    payload = {"event": "DRIVE_ENDED", "team_id": team_id, "ended_by": user_name}
    await manager.broadcast_to_team(team_id, payload)
    return payload

@app.post("/team/{team_id}/parking/start")
async def start_parking(team_id: str, user_name: str, location: str = "", db: Session = Depends(get_db)):
    """A팀 1번이 주차 시작"""
    state = get_team_state(db, team_id)
    if state.is_parked:
        raise HTTPException(status_code=400, detail="이미 주차 중입니다.")

    now = datetime.now()
    log = ParkingLog(team_id=team_id, parked_by=user_name, start_time=now, location=location)
    db.add(log)
    db.flush()

    state.is_parked = True
    state.parked_by = user_name
    state.current_parking_id = log.id
    db.commit()

    payload = {"event": "PARKING_STARTED", "team_id": team_id, "parked_by": user_name, "location": location}
    await manager.broadcast_to_team(team_id, payload)
    return payload

@app.post("/team/{team_id}/parking/end")
async def end_parking(team_id: str, user_name: str, db: Session = Depends(get_db)):
    """A팀 2번이 주차 종료"""
    state = get_team_state(db, team_id)
    if not state.is_parked:
        raise HTTPException(status_code=400, detail="주차 상태가 아닙니다.")

    now = datetime.now()
    log = db.query(ParkingLog).filter(ParkingLog.id == state.current_parking_id).first()
    if log:
        log.end_time = now
        log.unparked_by = user_name

    state.is_parked = False
    state.parked_by = None
    state.current_parking_id = None
    db.commit()

    payload = {"event": "PARKING_ENDED", "team_id": team_id, "unparked_by": user_name}
    await manager.broadcast_to_team(team_id, payload)
    return payload

# ==========================================
# 4. 전체 기록 조회 및 수정 API (모든 팀 통합)
# ==========================================

@app.get("/logs/all")
async def get_all_logs(db: Session = Depends(get_db)):
    """A팀, B팀 등 모든 팀의 기록을 한꺼번에 조회"""
    drives = db.query(DriveLog).all()
    parkings = db.query(ParkingLog).all()
    return {"drive_logs": drives, "parking_logs": parkings}

@app.patch("/logs/drive/{log_id}")
async def update_drive_log(log_id: int, update_data: LogUpdateSchema, db: Session = Depends(get_db)):
    """주행 기록 수정 (주행거리, 메모 등)"""
    log = db.query(DriveLog).filter(DriveLog.id == log_id).first()
    if not log:
        raise HTTPException(status_code=404, detail="기록을 찾을 수 없습니다.")

    if update_data.distance_km is not None: log.distance_km = update_data.distance_km
    if update_data.memo is not None: log.memo = update_data.memo

    db.commit()
    db.refresh(log)
    return {"status": "success", "updated_log": log}

@app.patch("/logs/parking/{log_id}")
async def update_parking_log(log_id: int, update_data: LogUpdateSchema, db: Session = Depends(get_db)):
    """주차 기록 수정 (주차 위치, 메모 등)"""
    log = db.query(ParkingLog).filter(ParkingLog.id == log_id).first()
    if not log:
        raise HTTPException(status_code=404, detail="기록을 찾을 수 없습니다.")

    if update_data.location is not None: log.location = update_data.location
    if update_data.memo is not None: log.memo = update_data.memo

    db.commit()
    db.refresh(log)
    return {"status": "success", "updated_log": log}

# ==========================================
# 5. 웹소켓 엔드포인트
# ==========================================
@app.websocket("/ws/{team_id}")
async def websocket_endpoint(websocket: WebSocket, team_id: str):
    await manager.connect(team_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(team_id, websocket)