from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app import crud
from app.dependencies import get_db
from app.schemas import EventDetail, EventListResponse

router = APIRouter(prefix="/events", tags=["events"])


@router.get("/", response_model=EventListResponse)
def list_events(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> EventListResponse:
    """Return a paginated list of received webhook events, newest first."""
    items, total = crud.get_events(db, limit=limit, offset=offset)
    return EventListResponse(total=total, limit=limit, offset=offset, items=items)


@router.get("/{event_id}", response_model=EventDetail)
def get_event(event_id: UUID, db: Session = Depends(get_db)) -> EventDetail:
    """Return the full detail of a single webhook event."""
    event = crud.get_event(db, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event
