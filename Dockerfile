# Use an official Python runtime as a base image
FROM python:3.12

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Copy the ngrok.exe executable into the container
COPY ngrok.exe .


# Copy the virtual environment (env) directory into the container
COPY env/ ./env/

# Install any needed packages specified in requirements.txt
RUN . ./env/Scripts/activate && pip install --no-cache-dir -r requirements.txt

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Install tmux
RUN apt-get update && apt-get install -y tmux

# Define the command to run the app
CMD ["tmux", "new-session", "-d", "-s", "my_session", "uvicorn src.FastApi.API:app --port 8000 && ngrok start my_tunnel"]
