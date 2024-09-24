with events_enriched as (
    select 
        concat(device_id, ':', session_id) as full_session_id
        , session_id
        , event_id
        , event_time as event_start_time
        , lag(event_time) over(partition by session_id order by event_time desc) as event_end_time
        , timediff(seconds, event_start_time, event_end_time) as event_duration_seconds
        , view_id
        , event_type
        , case
            when source_properties:url:path::string ilike '%course%' then true
            else false
          end as is_course_event
        , case
            when source_properties:url:path::string ilike '%task%' then true
            else false
          end as is_task_event
        , case  
            when source_properties:url:path::string ilike '%course%' then source_properties:url:query:id:values[0]::string
            else null
          end as course_id
    from 
        {{ ref("events") }}
        -- SEGMENT.FULLSTORY_25WP4.EVENTS
    order by
        event_time desc
)

, course_aggregates as (
    select
        full_session_id
        , course_id
        , sum(iff(is_course_event=true, event_duration_seconds, 0)) as course_duration
    from
        events_enriched
    where
        course_id is not null
    group by
        1,2
)

, course_session_aggregates as (
    select
        full_session_id
        , sum(course_duration) as course_event_duration_seconds
        , array_agg(object_construct('course_id', course_id, 'duration', course_duration)) as course_metadata
    from
        course_aggregates 
    group by
        1
)

, task_session_aggregates as (
    select
        full_session_id
        , sum(event_duration_seconds) as task_event_duration_seconds
    from
        events_enriched
    where
        is_task_event = true
    group by
        1
)

select
    e.full_session_id
    , c.course_event_duration_seconds
    , c.course_metadata
    , t.task_event_duration_seconds
from
    events_enriched e 
    left join course_session_aggregates c using(full_session_id)
    left join task_session_aggregates t using(full_session_id)
where
    coalesce(e.is_course_event, e.is_task_event) = true
