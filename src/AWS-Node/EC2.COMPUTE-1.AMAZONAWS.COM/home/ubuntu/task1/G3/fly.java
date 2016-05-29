
class Fly implements WritableComparable<Fly>{

	private String Origin;
	private String Destination;
	private String Carrier;
	private String Date;
	private String DepartTime;
	private String ArrivalTime;
	private Float ArrDelay;

	public Fly(String Origin, String Destination, String Carrier, String Date, String DepartTime,
			String ArrivalTime, Float ArrDelay) throws IOException {

		this.Origin = Origin;
		this.Destination = Destination;
		this.Carrier = Carrier;
		this.Date = Date;
		this.DepartTime = DepartTime;
		this.ArrivalTime = ArrivalTime;
		this.ArrDelay = ArrDelay;

	}

	public void write(DataOutput out) throws IOException {

		out.writeUTF(Origin);
		out.writeUTF(Destination);
		out.writeUTF(Carrier);
		out.writeUTF(Date);
		out.writeUTF(DepartTime);
		out.writeUTF(ArrivalTime);
		out.writeFloat(ArrDelay);

	}

	public void readFields(DataInput in) throws IOException {

		Origin = in.readUTF();
		Destination = in.readUTF();
		Carrier = in.readUTF();
		Date = in.readUTF();
		DepartTime = in.readUTF();
		ArrivalTime = in.readUTF();
		ArrDelay = in.readFloat();
	}
	@Override
	public int compareTo(Fly o) {
		String thisValue = this.Origin;
		String thatValue =  o.Origin;
		return (thisValue.compareTo(o.Origin));
	}

	public int hashCode() {
		int result = 1;
		result = Origin.hashCode() + Destination.hashCode();
		result = String.valueOf(result).hashCode() + Carrier.hashCode();
		result = String.valueOf(result).hashCode() + Date.hashCode();
		result = String.valueOf(result).hashCode() + DepartTime.hashCode();
		result = String.valueOf(result).hashCode() + ArrivalTime.hashCode();

		return String.valueOf(result).hashCode();
	}

	@Override
	public String toString() {
		return "[" + Origin + "," + Destination + "," + Carrier + "," + Date + "," + DepartTime + "," + ArrivalTime
				+ "," + ArrDelay.toString() + "]";
	}

	public String getOrigin()
    {
    	return Origin;
    }

	public String getDestination()
    {
    	return Destination;
    }

	public String getCarrier()
    {
    	return Carrier;
    }

	public String getDate()
    {
    	return Date;
    }

	public String getDepartTime()
    {
    	return DepartTime;
    }

	public String getArrivalTime()
    {
    	return ArrivalTime;
    }

	public Float getArrDelay()
    {
    	return ArrDelay;
    }
}
