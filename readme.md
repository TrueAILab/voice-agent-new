# Simple Render + Twilio Setup

If you want the easiest setup:

1. Push this project to GitHub
2. Connect that GitHub repo to Render
3. Let Render use the included `Dockerfile`
4. Add the environment variables
5. Put the Render URL into Twilio
6. Call your Twilio number

That is all.

## Which File Is For Production

Use `server.py` for Twilio phone calls.

Do not deploy `agent.py` to Render for phone calls. `agent.py` is only for local mic testing on your laptop.

## Files Already Included

These files are already prepared for deployment:

- `server.py`
- `Dockerfile`
- `requirements-server.txt`
- `render.yaml`
- `env.example`

## Render Deployment

### 1. Push to GitHub

Push this folder to a GitHub repo.

### 2. Create a New Render Web Service

In Render:

1. Click `New +`
2. Click `Web Service`
3. Connect your GitHub account
4. Select this repository

### 3. Use Docker

When Render asks how to deploy:

- Choose `Docker`

Render will automatically use the `Dockerfile` in this repo.

You do not need to manually install packages on Render.

## Environment Variables In Render

Add these in the Render dashboard:

- `GEMINI_API_KEY=your_gemini_api_key_here`
- `GEMINI_MODEL=models/gemini-3.1-flash-live-preview`
- `N8N_WEBHOOK_URL=https://your-n8n-instance.com/webhook/your-webhook-id`
- `PUBLIC_BASE_URL=https://your-service-name.onrender.com`
- `PORT=10000`

Notes:

- `PUBLIC_BASE_URL` must be your actual Render public URL
- `PORT` should stay `10000`

## What Render Will Run

The included `Dockerfile` runs:

```bash
uvicorn server:app --host 0.0.0.0 --port ${PORT:-10000}
```

So Render will start the Twilio bridge automatically.

## After Render Deploys

Once the deploy finishes, open:

- `https://your-service-name.onrender.com/`
- `https://your-service-name.onrender.com/healthz`

If `/healthz` returns OK, the service is up.

## Twilio Setup

In Twilio Console:

1. Open your phone number
2. Go to `Voice Configuration`
3. For `A Call Comes In`, set:

```text
https://your-service-name.onrender.com/twilio/voice
```

4. Set method to `HTTP POST`
5. Save

That is the only Twilio webhook you need for inbound calls.

## What Happens On A Call

When someone calls your Twilio number:

1. Twilio hits `/twilio/voice`
2. Your server returns TwiML
3. Twilio opens a WebSocket to `/twilio/media`
4. Caller audio goes to Gemini
5. Gemini audio comes back to Twilio
6. The agent speaks to the caller

## TwiML

Your app generates the TwiML automatically.

It looks like this:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream
      url="wss://your-service-name.onrender.com/twilio/media"
      statusCallback="https://your-service-name.onrender.com/twilio/stream-status"
      statusCallbackMethod="POST"
    >
      <Parameter name="agent" value="trueai-gemini" />
    </Stream>
  </Connect>
</Response>
```

You do not need to paste this in Twilio manually.

## Fastest End-To-End Checklist

1. Push repo to GitHub
2. Create Docker web service in Render
3. Add env vars
4. Deploy
5. Copy Render URL
6. Put `https://your-render-url/twilio/voice` into Twilio number webhook
7. Call the Twilio number from your mobile

## Troubleshooting

### No voice from the bot

Check:

- `GEMINI_API_KEY` is valid
- `GEMINI_MODEL` is exactly `models/gemini-3.1-flash-live-preview`
- Render logs show the app started
- `/healthz` works

### Twilio webhook fails

Check:

- You used `https://your-service-name.onrender.com/twilio/voice`
- The Render service is live

### Bot talks but lead is not saved

Check:

- `N8N_WEBHOOK_URL` is correct
- The n8n webhook is reachable

## Summary

The simple deployment flow is:

- GitHub -> Render -> Dockerfile -> add env vars -> Twilio webhook -> call the number
