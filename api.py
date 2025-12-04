"""
FastAPI wrapper for Google Maps Lead Scraper
Run: python -m uvicorn api:app --reload
"""
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from typing import Optional, List
import uuid
import os
from datetime import datetime
import re
import threading

from maps_scraper import scrape_maps_with_progress

app = FastAPI(
    title="Lead Generator API",
    description="Scrape Google Maps for business leads",
    version="1.0.0"
)

# Store for tracking scraping jobs
jobs = {}


class ScrapeRequest(BaseModel):
    query: str


class ScrapeResponse(BaseModel):
    job_id: str
    status: str
    message: str


def deduplicate_results(results):
    """Remove duplicates based on business_name and address"""
    seen = set()
    unique = []
    for r in results:
        key = (r.get('business_name', ''), r.get('address', ''))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


def progress_callback(job_id: str, event: str, data: dict):
    """Callback to update job progress"""
    if job_id not in jobs:
        return
    
    jobs[job_id]["logs"].append({
        "time": datetime.now().strftime('%H:%M:%S'),
        "event": event,
        "data": data
    })
    
    if event == "scrolling":
        jobs[job_id]["phase"] = "scrolling"
        jobs[job_id]["scroll_count"] = data.get("count", 0)
    elif event == "listings_found":
        jobs[job_id]["phase"] = "extracting"
        jobs[job_id]["total_listings"] = data.get("total", 0)
    elif event == "extracting":
        jobs[job_id]["phase"] = "extracting"
        jobs[job_id]["current"] = data.get("current", 0)
        jobs[job_id]["total_listings"] = data.get("total", 0)
        jobs[job_id]["collected"] = data.get("collected", 0)


def run_scraper(job_id: str, query: str):
    """Background task to run the scraper"""
    jobs[job_id]["status"] = "running"
    jobs[job_id]["phase"] = "starting"
    jobs[job_id]["logs"].append({
        "time": datetime.now().strftime('%H:%M:%S'),
        "event": "started",
        "data": {"message": f"Starting scrape for: {query}"}
    })
    
    try:
        callback = lambda event, data: progress_callback(job_id, event, data)
        results = scrape_maps_with_progress(query, callback)
        
        if results:
            results = deduplicate_results(results)
            
            import pandas as pd
            df = pd.DataFrame(results)
            
            os.makedirs("leads", exist_ok=True)
            safe_query = re.sub(r'[^\w\s-]', '', query).replace(' ', '_')[:30]
            date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"leads/leads_{safe_query}_{date_str}.csv"
            
            df.to_csv(filename, index=False)
            
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["phase"] = "completed"
            jobs[job_id]["total_leads"] = len(results)
            jobs[job_id]["csv_file"] = filename
            jobs[job_id]["results"] = results
            jobs[job_id]["logs"].append({
                "time": datetime.now().strftime('%H:%M:%S'),
                "event": "completed",
                "data": {"message": f"Completed! Found {len(results)} unique leads"}
            })
        else:
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["phase"] = "completed"
            jobs[job_id]["total_leads"] = 0
            jobs[job_id]["results"] = []
            
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["phase"] = "failed"
        jobs[job_id]["error"] = str(e)
        jobs[job_id]["logs"].append({
            "time": datetime.now().strftime('%H:%M:%S'),
            "event": "error",
            "data": {"message": str(e)}
        })


@app.post("/scrape", response_model=ScrapeResponse)
async def start_scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """Start a new scraping job (runs in background)"""
    job_id = str(uuid.uuid4())
    
    jobs[job_id] = {
        "status": "pending",
        "phase": "pending",
        "query": request.query,
        "total_leads": None,
        "total_listings": 0,
        "current": 0,
        "collected": 0,
        "scroll_count": 0,
        "csv_file": None,
        "results": None,
        "error": None,
        "logs": [],
        "started_at": datetime.now().isoformat()
    }
    
    background_tasks.add_task(run_scraper, job_id, request.query)
    
    return ScrapeResponse(
        job_id=job_id,
        status="pending",
        message=f"Scraping job started for query: {request.query}"
    )


@app.get("/job/{job_id}")
async def get_job_status(job_id: str):
    """Check status of a scraping job with progress"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    return {
        "job_id": job_id,
        "status": job["status"],
        "phase": job.get("phase", "unknown"),
        "query": job["query"],
        "total_leads": job["total_leads"],
        "total_listings": job.get("total_listings", 0),
        "current": job.get("current", 0),
        "collected": job.get("collected", 0),
        "scroll_count": job.get("scroll_count", 0),
        "csv_file": job["csv_file"],
        "error": job["error"],
        "logs": job.get("logs", [])[-20:]  # Last 20 logs
    }


@app.get("/job/{job_id}/results")
async def get_job_results(job_id: str):
    """Get full results of a completed job"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job is {job['status']}, not completed yet")
    
    return {
        "job_id": job_id,
        "query": job["query"],
        "total_leads": job["total_leads"],
        "leads": job["results"]
    }


@app.get("/job/{job_id}/download")
async def download_csv(job_id: str):
    """Download CSV file for a completed job"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job is {job['status']}, not completed yet")
    
    if not job["csv_file"] or not os.path.exists(job["csv_file"]):
        raise HTTPException(status_code=404, detail="CSV file not found")
    
    return FileResponse(
        job["csv_file"],
        media_type="text/csv",
        filename=os.path.basename(job["csv_file"])
    )


@app.get("/jobs")
async def list_jobs():
    """List all scraping jobs"""
    return {
        "total_jobs": len(jobs),
        "jobs": [
            {
                "job_id": jid,
                "status": j["status"],
                "query": j["query"],
                "total_leads": j["total_leads"],
                "started_at": j.get("started_at")
            }
            for jid, j in jobs.items()
        ]
    }


@app.delete("/job/{job_id}")
async def delete_job(job_id: str):
    """Delete a job and its CSV file"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    
    # Delete CSV file if exists
    if job.get("csv_file") and os.path.exists(job["csv_file"]):
        try:
            os.remove(job["csv_file"])
        except Exception as e:
            pass  # Ignore file deletion errors
    
    # Remove job from memory
    del jobs[job_id]
    
    return {"success": True, "message": f"Job {job_id} deleted successfully"}


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the frontend"""
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api")
async def api_info():
    """API info"""
    return {
        "name": "Lead Generator API",
        "version": "1.0.0"
    }
