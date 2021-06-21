import sys
import time
from kafka import KafkaProducer
import cv2

TOPIC = 'video1'


def publish_video(file):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    video = cv2.VideoCapture(file)

    while (video.isOpened()):
        success, frame = video.read()
        if not success:
            break
        ret, buf = cv2.imencode('.jpg', frame)

        producer.send(TOPIC, buf.tobytes())
        time.sleep(0.2)
    video.release()
    print('publish complete!')


def publish_camera():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    camera = cv2.VideoCapture(0)

    try:
        while (True):
            success, frame = camera.read()

            ret, buf = cv2.imencode('.jpg', frame)
            producer.send(TOPIC, buf.tobytes())

            # Choppier stream, reduced load on processor
            time.sleep(0.2)

    except:
        print("\nExiting.")
        sys.exit(1)

    camera.release()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        video_path = sys.argv[1]
        print(video_path)
        publish_video(video_path)
    else:
        publish_camera()


