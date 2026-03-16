import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import Groq from "groq-sdk";
dotenv.config();

const app  = express();
const groq = new Groq({ apiKey: process.env.GROQ_API_KEY });

app.use(cors());
app.use(express.json());

/* ── Health check ── */
app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "ImpactGrid Dijo", ts: new Date().toISOString() });
});

/* ── Chat endpoint ── */
app.post("/chat", async (req, res) => {
  try {
    const { message, mode = "adviser" } = req.body;
    if (!message) return res.status(400).json({ error: "No message provided" });

    const systemPrompts = {
      adviser: `You are Dijo, ImpactGrid's AI financial adviser for small business owners. 
You speak plainly and directly — no jargon, no waffle. 
You give specific, actionable financial advice based on the data provided.
Always be honest even if the news is tough. Keep responses concise and practical.
Format responses clearly with short paragraphs. Use bullet points sparingly.`,

      dashboard: `You are Dijo, an AI financial analyst embedded in ImpactGrid.
Analyse the financial data provided and give clear, specific insights.
Focus on trends, risks, and opportunities. Be direct and numbers-focused.`,

      group: `You are Dijo, ImpactGrid's AI adviser for the ImpactGrid Group platform.
You help business owners and investors understand financial performance.
Be professional, clear, and actionable.`,

      creator: `You are the ImpactGrid Creator Intelligence Engine — a data-driven AI that helps creators and businesses identify viral content opportunities.
You analyse trends and generate optimised content for TikTok, YouTube, and Instagram.
When asked to generate content, respond with a hook, caption, hashtags, and posting advice.
Be specific, direct, and data-driven. No generic advice.`
    };

    const systemPrompt = systemPrompts[mode] || systemPrompts.adviser;

    const completion = await groq.chat.completions.create({
      model:       "llama-3.3-70b-versatile",
      max_tokens:  1024,
      temperature: 0.7,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user",   content: message }
      ]
    });

    const reply = completion.choices[0]?.message?.content || "I couldn't generate a response.";
    res.json({ reply });

  } catch (err) {
    console.error("[Dijo] Error:", err.message);
    res.status(500).json({ error: "AI service error", details: err.message });
  }
});

/* ── Keep-warm ping ── */
app.get("/ping", (req, res) => res.json({ pong: true }));


/* ================================================================
   TIKTOK ROUTES
   Requires these environment variables set on Render:
     TIKTOK_CLIENT_KEY
     TIKTOK_CLIENT_SECRET
     TIKTOK_REDIRECT_URI  (https://impactgridgroup.com/tiktok-callback.html)
================================================================ */

/* ── ROUTE 1: Token Exchange ── */
app.post("/tiktok/token", async (req, res) => {
  const { code, redirect_uri, code_verifier } = req.body;

  if (!code || !code_verifier) {
    return res.status(400).json({ error: "Missing code or code_verifier" });
  }

  try {
    const response = await fetch("https://open.tiktokapis.com/v2/oauth/token/", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_key:    process.env.TIKTOK_CLIENT_KEY,
        client_secret: process.env.TIKTOK_CLIENT_SECRET,
        code:          code,
        grant_type:    "authorization_code",
        redirect_uri:  redirect_uri || process.env.TIKTOK_REDIRECT_URI,
        code_verifier: code_verifier
      }).toString()
    });

    const data = await response.json();

    if (!response.ok || data.error) {
      console.error("[TikTok /token] Error:", data);
      return res.status(400).json({ error: data.error || "Token exchange failed", details: data });
    }

    res.json({
      access_token:  data.access_token,
      open_id:       data.open_id,
      expires_in:    data.expires_in,
      refresh_token: data.refresh_token,
      scope:         data.scope
    });

  } catch (err) {
    console.error("[TikTok /token] Exception:", err.message);
    res.status(500).json({ error: "Token exchange failed", details: err.message });
  }
});


/* ── ROUTE 2: User Profile (Display API) ── */
app.post("/tiktok/profile", async (req, res) => {
  const { access_token } = req.body;
  if (!access_token) return res.status(400).json({ error: "Missing access_token" });

  try {
    const response = await fetch(
      "https://open.tiktokapis.com/v2/user/info/?fields=open_id,union_id,avatar_url,display_name,username,follower_count,following_count,likes_count,video_count",
      {
        method: "GET",
        headers: { "Authorization": "Bearer " + access_token }
      }
    );

    const data = await response.json();

    if (!response.ok || data.error) {
      console.error("[TikTok /profile] Error:", data);
      return res.status(400).json({ error: "Failed to fetch profile", details: data });
    }

    res.json(data);

  } catch (err) {
    console.error("[TikTok /profile] Exception:", err.message);
    res.status(500).json({ error: "Profile fetch failed", details: err.message });
  }
});


/* ── ROUTE 3: User Videos (Display API) ── */
app.post("/tiktok/videos", async (req, res) => {
  const { access_token, max_count } = req.body;
  if (!access_token) return res.status(400).json({ error: "Missing access_token" });

  try {
    const response = await fetch(
      "https://open.tiktokapis.com/v2/video/list/?fields=id,title,cover_image_url,video_description,duration,like_count,comment_count,share_count,view_count",
      {
        method: "POST",
        headers: {
          "Authorization": "Bearer " + access_token,
          "Content-Type":  "application/json"
        },
        body: JSON.stringify({ max_count: max_count || 10 })
      }
    );

    const data = await response.json();

    if (!response.ok || data.error) {
      console.error("[TikTok /videos] Error:", data);
      return res.status(400).json({ error: "Failed to fetch videos", details: data });
    }

    res.json(data);

  } catch (err) {
    console.error("[TikTok /videos] Exception:", err.message);
    res.status(500).json({ error: "Videos fetch failed", details: err.message });
  }
});


/* ── ROUTE 4: Publish Video (Content Posting API) ── */
app.post("/tiktok/publish", async (req, res) => {
  const { access_token, video_url, caption, privacy_level } = req.body;

  if (!access_token || !video_url) {
    return res.status(400).json({ error: "Missing access_token or video_url" });
  }

  try {
    const response = await fetch("https://open.tiktokapis.com/v2/post/publish/video/init/", {
      method: "POST",
      headers: {
        "Authorization": "Bearer " + access_token,
        "Content-Type":  "application/json"
      },
      body: JSON.stringify({
        post_info: {
          title:           caption || "",
          privacy_level:   privacy_level || "PUBLIC_TO_EVERYONE",
          disable_duet:    false,
          disable_comment: false,
          disable_stitch:  false
        },
        source_info: {
          source:    "PULL_FROM_URL",
          video_url: video_url
        }
      })
    });

    const data = await response.json();

    if (!response.ok || (data.error && data.error.code !== "ok")) {
      console.error("[TikTok /publish] Error:", data);
      return res.status(400).json({ error: "Publish failed", details: data });
    }

    res.json({
      publish_id: data.data?.publish_id,
      status:     "publishing"
    });

  } catch (err) {
    console.error("[TikTok /publish] Exception:", err.message);
    res.status(500).json({ error: "Publish failed", details: err.message });
  }
});


/* ── ROUTE 5: Share Kit ── */
app.post("/tiktok/share", async (req, res) => {
  const { access_token, video_url, title } = req.body;

  if (!access_token || !video_url) {
    return res.status(400).json({ error: "Missing access_token or video_url" });
  }

  try {
    /* Verify token is still valid */
    const verify = await fetch(
      "https://open.tiktokapis.com/v2/user/info/?fields=open_id",
      { headers: { "Authorization": "Bearer " + access_token } }
    );

    if (!verify.ok) {
      return res.status(401).json({ error: "Invalid or expired access token" });
    }

    res.json({
      share_url:  video_url,
      title:      title || "",
      client_key: process.env.TIKTOK_CLIENT_KEY,
      status:     "ready"
    });

  } catch (err) {
    console.error("[TikTok /share] Exception:", err.message);
    res.status(500).json({ error: "Share failed", details: err.message });
  }
});


/* ── ROUTE 6: Refresh Token ── */
app.post("/tiktok/refresh", async (req, res) => {
  const { refresh_token } = req.body;
  if (!refresh_token) return res.status(400).json({ error: "Missing refresh_token" });

  try {
    const response = await fetch("https://open.tiktokapis.com/v2/oauth/token/", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_key:    process.env.TIKTOK_CLIENT_KEY,
        client_secret: process.env.TIKTOK_CLIENT_SECRET,
        grant_type:    "refresh_token",
        refresh_token: refresh_token
      }).toString()
    });

    const data = await response.json();

    if (!response.ok || data.error) {
      return res.status(400).json({ error: "Token refresh failed", details: data });
    }

    res.json({
      access_token:  data.access_token,
      expires_in:    data.expires_in,
      refresh_token: data.refresh_token
    });

  } catch (err) {
    console.error("[TikTok /refresh] Exception:", err.message);
    res.status(500).json({ error: "Token refresh failed", details: err.message });
  }
});

/* ================================================================
   END TIKTOK ROUTES
================================================================ */


const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`ImpactGrid Dijo running on port ${PORT}`);
});
