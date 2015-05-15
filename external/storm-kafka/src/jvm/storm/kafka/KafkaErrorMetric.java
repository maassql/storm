package storm.kafka;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.HashMap;

import backtype.storm.metric.api.IMetric;

public class KafkaErrorMetric
{
	String _metricPath = "";
	RubeGoldbergMetrics _rubyMetrics;
	public KafkaErrorMetric ( String metricPath )
	{
		_metricPath = metricPath;
		_rubyMetrics = new RubeGoldbergMetrics();
		
		// for every single KafkaError in KafkaError.java, at the very least, send a zero every poll
		for (KafkaError ke : KafkaError.values())
		{
			_rubyMetrics.initialize(metricPath() + "/ke/" + ke.toString());
		}
	}
	
	public void incrByKafkaError(short fetchResponseErrorCode)
	{
		KafkaError kError = KafkaError.getError(fetchResponseErrorCode);
        _rubyMetrics.incr(metricPath() + "/ke/" + kError.toString());
	}
	
	public void incrByException(Exception e)
	{
		String errorClass = e.getClass().getName();
		String errorClass2 = "";
        if (e instanceof ConnectException )
        { errorClass2 = "ConnectException" ;}
        else if ( e instanceof SocketTimeoutException )
        { errorClass2 = "SocketTimeoutException" ;}
        else if ( e instanceof IOException )
        { errorClass2 = "IOException" ;}
        else if (  e instanceof UnresolvedAddressException )
        { errorClass2 = "UnresolvedAddressException" ;}
        else { errorClass2 = "other" ;}
        
        _rubyMetrics.incr(metricPath() + "/1/" + errorClass);
        _rubyMetrics.incr(metricPath() + "/2/" + errorClass2);
	}
	
	
	private String metricPath()
	{
		return _metricPath + "/" + "KafkaErrors";
	}
	
	public HashMap<String, Double> getMetricsAndReset() 
	{
		return _rubyMetrics.getMetricsAndReset();
	}
	
	public RubeGoldbergMetrics RubyMetrics() { return this._rubyMetrics ; }

}
