import json
import asyncio

from datetime import datetime, timedelta
from typing import Annotated, Optional, List

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, SecretStr
from motor.motor_asyncio import AsyncIOMotorClient
from kafka import KafkaProducer
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from threading import Thread

# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Initialize Kafka producer
KAFKA_SERVER = "kafka:9092"
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

# MongoDB configuration
MONGO_URI = "mongodb://mongodb:27017"
client = AsyncIOMotorClient(MONGO_URI)
db = client["mydatabase"]
collection = db["users"]


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = False
    is_admin: Optional[bool] = False


class UserInDB(User):
    hashed_password: str
    
class UserCreate(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    password: SecretStr
    disabled: Optional[bool] = False
    is_admin: Optional[bool] = False

# Async function to process messages from Kafka and insert into MongoDB
async def consume_and_process():
    try:
        consumer = AIOKafkaConsumer(
            "user-topic",
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda m: m.decode('utf-8')
        )
        await consumer.start()

        async for msg in consumer:
            user_data = json.loads(msg.value)
            print("Received data:", user_data)
            await collection.insert_one(user_data)
    except KafkaError as e:
        print(f"Kafka error: {e}. Retrying in 5 seconds.")
        await asyncio.sleep(5)


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Start the Kafka consumer in a separate thread
consumer_thread = Thread(target=consume_and_process)
consumer_thread.start()

app = FastAPI()


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


async def get_user(username: str):
    user = await collection.find_one({"username": username})
    if user:
        return UserInDB(**user)


async def authenticate_user(username: str, password: str):
    user = await get_user(username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = await get_user(username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_admin(
    current_user: Annotated[User, Depends(get_current_user)]
):

    if not current_user.is_admin:
        raise HTTPException(status_code=400, detail="Not an admin user")
    return current_user


@app.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


# Start the Kafka consumer
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_and_process())
    
@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

@app.post("/register", response_model=User, status_code=status.HTTP_201_CREATED)
async def register(user: UserCreate):
    existing_user = await collection.find_one({"username": user.username})
    if existing_user:
        raise HTTPException(
            status_code=400,
            detail=f"User with username '{user.username}' already exists"
        )

    hashed_password = get_password_hash(user.password.get_secret_value())
    user_data = {
        "username": user.username,
        "full_name": user.full_name,
        "email": user.email,
        "disabled": user.disabled,
        "hashed_password": hashed_password,
        "is_admin": user.is_admin
    }
    

    producer.send("user-topic", json.dumps(user_data).encode("utf-8"))
    return {**user.dict(), "disabled": user.disabled, "password": "****", "is_admin": user.is_admin}

@app.get("/users/me/", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user

@app.post("/create-user", response_model=User)
async def create_user(user: UserCreate, current_user: Annotated[User, Depends(get_current_active_admin)]):
    # Check if the current user is an admin
    admin_user = await collection.find_one({"username": current_user.username})
    if not admin_user:
        raise HTTPException(status_code=403, detail="Operation not permitted")

    # Check if user already exists in MongoDB
    existing_user = await collection.find_one({"username": user.username})
    if existing_user:
        raise HTTPException(
            status_code=400,
            detail=f"User with username '{user.username}' already exists"
        )
    # Transfer data to Kafka
    hashed_password = get_password_hash(user.password.get_secret_value())
    kafka_data = {
        "username": user.username,
        "full_name": user.full_name,
        "email": user.email,
        "disabled": user.disabled,
        "hashed_password": hashed_password,
        "is_admin": user.is_admin 
    }

    producer.send("user-topic", json.dumps(kafka_data).encode("utf-8"))
    return {**user.dict(), "disabled": user.disabled, "password": "****", "is_admin": user.is_admin}

@app.get("/users", response_model=List[User])
async def get_all_users(current_user: Annotated[User, Depends(get_current_active_admin)]):
    user = await collection.find_one({"username": current_user.username})
    if not user:
        raise HTTPException(status_code=403, detail="Operation not permitted")
    users = await collection.find().to_list(None)
    return users

