from typing import Dict, List, Any
from src.services.analytics_service.olap_store import olap_store

class AnalyticsAggregator:
    """
    ANA-04: Utilization and booking trend aggregation queries.
    Executes OLAP queries against DuckDB columnar tables.
    """
    @staticmethod
    def get_resource_utilization() -> List[Dict[str, Any]]:
        """
        Calculates total hours booked and utilization rate per resource.
        Assumes an operational standard of 50 hours available per resource per week.
        """
        sql = """
        SELECT 
            b.resource_id,
            COUNT(b.booking_id) as total_bookings,
            COALESCE(SUM(b.duration_hours), 0.0) as total_hours_booked,
            COALESCE(AVG(b.attendees_count), 0.0) as avg_attendees,
            LEAST(100.0, ROUND((COALESCE(SUM(b.duration_hours), 0.0) / 50.0) * 100.0, 2)) as utilization_rate_pct
        FROM fact_bookings b
        WHERE b.booking_id NOT IN (SELECT booking_id FROM fact_cancellations)
        GROUP BY b.resource_id
        ORDER BY total_hours_booked DESC
        """
        rows = olap_store.execute_query(sql)
        return [
            {
                "resource_id": r[0],
                "total_bookings": r[1],
                "total_hours_booked": round(r[2], 2),
                "avg_attendees": round(r[3], 1),
                "utilization_rate_pct": r[4]
            }
            for r in rows
        ]

    @staticmethod
    def get_peak_hours_analysis() -> List[Dict[str, Any]]:
        """
        Analyzes booking start time distribution across hours of the day (0-23).
        """
        sql = """
        SELECT 
            SUBSTRING(start_time, 12, 2) as hour_of_day,
            COUNT(*) as bookings_count
        FROM fact_bookings
        GROUP BY hour_of_day
        ORDER BY hour_of_day ASC
        """
        rows = olap_store.execute_query(sql)
        results = []
        for r in rows:
            hour_str = r[0] or "09"
            results.append({
                "hour": f"{hour_str}:00",
                "bookings_count": r[1]
            })
        return results

    @staticmethod
    def get_department_distribution() -> List[Dict[str, Any]]:
        """
        Aggregates booking volumes and attendees across academic departments.
        """
        sql = """
        SELECT 
            department,
            COUNT(*) as total_bookings,
            COALESCE(SUM(attendees_count), 0) as total_attendees
        FROM fact_bookings
        GROUP BY department
        ORDER BY total_bookings DESC
        """
        rows = olap_store.execute_query(sql)
        return [
            {
                "department": r[0] or "General",
                "total_bookings": r[1],
                "total_attendees": r[2]
            }
            for r in rows
        ]

    @staticmethod
    def get_cancellation_rates() -> Dict[str, Any]:
        """
        Computes overall booking conversion vs cancellation metrics.
        """
        sql_confirmed = "SELECT COUNT(*) FROM fact_bookings"
        sql_cancelled = "SELECT COUNT(*) FROM fact_cancellations"
        
        total_confirmed = olap_store.execute_query(sql_confirmed)[0][0]
        total_cancelled = olap_store.execute_query(sql_cancelled)[0][0]
        
        net_active = max(0, total_confirmed - total_cancelled)
        cancellation_rate = 0.0
        if total_confirmed > 0:
            cancellation_rate = round((total_cancelled / total_confirmed) * 100.0, 2)

        return {
            "gross_bookings": total_confirmed,
            "cancelled_bookings": total_cancelled,
            "net_active_bookings": net_active,
            "cancellation_rate_pct": cancellation_rate
        }

    @staticmethod
    def get_service_request_summary() -> Dict[str, Any]:
        sql = """
        SELECT 
            priority,
            COUNT(*) as count
        FROM fact_service_requests
        GROUP BY priority
        """
        rows = olap_store.execute_query(sql)
        breakdown = {r[0]: r[1] for r in rows}
        total = sum(breakdown.values())
        return {
            "total_tickets": total,
            "by_priority": breakdown
        }

analytics_aggregator = AnalyticsAggregator()
