use std::sync::{Arc,Mutex,RwLock};
use std::collections::HashMap;
use std::collections::VecDeque;

pub type QueueName = String;
pub struct Queue {
    inner: Arc<Mutex<VecDeque<String>>>
}
pub struct QueueTable {
    inner: Arc<RwLock<HashMap<QueueName, Queue>>>
}

fn get_queue_with_lock(lock: &HashMap<QueueName, Queue>, queue_name: &QueueName) -> Option<Queue> {
    let result = lock.get(queue_name);
    match result {
        Some(queue) => {
            Some(queue.clone())
        }
        None => None
    }
}

fn create_queue(lock: &mut HashMap<QueueName, Queue>, queue_name: QueueName) -> Queue {
    let queue = Queue::new();
    lock.insert(queue_name, queue.clone());
    return queue;
}

impl Queue {
    pub fn new() -> Queue {
        Queue { inner: Arc::new(Mutex::new(VecDeque::new())) }
    }

    pub fn push_back(&self, value: String) {
        let mut queue = self.inner.lock().unwrap();
        queue.push_back(value)
    }

    pub fn pop_front(&self) -> Option<String> {
        let mut queue = self.inner.lock().unwrap();
        queue.pop_front()
    }
}

impl Clone for Queue {
    fn clone(&self) -> Queue {
        Queue {
            inner: self.inner.clone()
        }
    }
}

impl QueueTable {
    pub fn new() -> QueueTable {
        QueueTable { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn get_or_create_queue(&self, queue_name: QueueName) -> Queue {
        {
            let result = self.get_queue(&queue_name);
            if result.is_some() {
                return result.unwrap();
            }
        }
        let mut write_lock = self.inner.write().unwrap();
        match get_queue_with_lock(&write_lock, &queue_name) {
            Some(queue) => {
                queue
            }
            None => {
                create_queue(&mut write_lock, queue_name)
            }
        }
    }

    pub fn get_queue(&self, queue_name: &QueueName) -> Option<Queue> {
        let read_lock = self.inner.read().unwrap();
        get_queue_with_lock(&read_lock, &queue_name)
    }
}

impl Clone for QueueTable {
    fn clone(&self) -> QueueTable {
        QueueTable {
            inner: self.inner.clone()
        }
    }
}
