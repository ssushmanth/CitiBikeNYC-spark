package util

object Distance {
  
  def calcDist(lat1: Double, lon1: Double,lat2: Double, lon2: Double): Int = {
		val AVERAGE_RADIUS_OF_EARTH_KM = 6371
        val latDistance = Math.toRadians(lat1 - lat2)
        val lngDistance = Math.toRadians(lon1 - lon2)
        val sinLat = Math.sin(latDistance / 2)
        val sinLng = Math.sin(lngDistance / 2)
        val a = sinLat * sinLat +
        (Math.cos(Math.toRadians(lat1)) *
            Math.cos(Math.toRadians(lat2)) *
            sinLng * sinLng)
        val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
        (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
  }
             
}