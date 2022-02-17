
CREATE PROCEDURE downloadCount 
	@ProcessId varchar(150),
	@ProcessStartDateTime datetime,
	@ProcessEndDateTime datetime,
	@ProcessingStatus varchar(100)	
AS
BEGIN
	
	SET NOCOUNT ON;
	Declare @countIndex int=0;


set @countIndex = (Select Count(ProcessId) from [dbo].[auditTable] where ProcessingStatus='Execution Successful');
	if @ProcessingStatus = 'Execution Successful'
		begin
		insert into [dbo].[auditTable] 
		values(@ProcessId, 
				@countIndex + 1,
				@ProcessStartDateTime,
				@ProcessEndDateTime,
				@ProcessingStatus)
		end
	else
		begin
			insert into [dbo].[auditTable] 
		values(@ProcessId, 
				@countIndex,
				@ProcessStartDateTime,
				@ProcessEndDateTime,
				@ProcessingStatus)
		
		end
    
END
GO
