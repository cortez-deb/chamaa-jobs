import Redis from 'ioredis';
import dotenv from 'dotenv'
dotenv.config();
const redis = new Redis("redis://default:5tEfpjGaSU1wMwLkilHuoOd6yUgqouUR@redis-15649.c17.us-east-1-4.ec2.cloud.redislabs.com:15649");
redis.on('error', err => console.log('Redis Client Error', err));
redis.on('connect', () => console.log('Redis Client Connected'));

export const connectRedis = async () => {
    try {
        await redis.ping();
        console.log('Redis connected successfully');
    } catch (error) {
        console.error('Failed to connect to Redis:', error);
        throw error;
    }
};

export class RedisQueues {
    static async addToQueue(queue, data) {
        try {
            console.log(data)
            const response = await redis.rpush(queue, JSON.stringify(data));
            return response;
        } catch (error) {
            console.error('Error adding to queue:', error);
            throw error;
        }
    }

    static async popRightFromQueue(queue) {
        try {
            const response = await redis.rpop(queue);
            return response ? JSON.parse(response) : null;
        } catch (error) {
            console.error('Error popping from right:', error);
            throw error;
        }
    }

    static async popLeftFromQueue(queue) {
        try {
            const response = await redis.lpop(queue);
            return response ? JSON.parse(response) : null;
        } catch (error) {
            console.error('Error popping from left:', error);
            throw error;
        }
    }

    static async peek(queue) {
        try {
            const response = await redis.lrange(queue, 0, -1);
            const data = response.map(item =>{
                if(item && item.length > 0){
                    try{
                        return JSON.parse(item);
                    }catch(e){
                        console.error('Error parsing queue item:', e);
                        return "{error: 'Invalid JSON'}";
                    }
                }                
                return "{error: 'Empty item'}";
            });
            return data;
        } catch (error) {
            console.error('Error peeking queue:', error);
            throw error;
        }
    }

    static async queueLength(queue) {
        try {
            const response = await redis.llen(queue);
            return response;
        } catch (error) {
            console.error('Error getting queue length:', error);
            throw error;
        }
    }
}

export default redis;