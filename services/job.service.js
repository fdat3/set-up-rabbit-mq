const jobQueueMQ = async (deal) => {
  try {
    const result = await Deal.create({
      id: deal.id,
      roomtypeid: deal.roomtypeid,
      dealdate: deal.dealdate,
      baseratebeforetax: deal.baseratebeforetax,
      baserateaftertax: deal.baserateaftertax,
      baserateaftertaxvnd: deal.baserateaftertaxvnd,
      baseratewholesale: deal.baseratewholesale,
      baseratewholesalevnd: deal.baseratewholesalevnd,
      pobaserate: deal.pobaserate,
      pobaseratevnd: deal.pobaseratevnd,
      commission: deal.commission,
      commissionvnd: deal.commissionvnd,
      commissionpercentage: deal.commissionpercentage
    });;

    return {
      error: false,
      data: result,
      code: 200,
      msg: "Insert successfully from service"
    };
  } catch (error) {
    return {
      error: true,
      data: null,
      code: 500,
      msg: `Failed to process job: ${error.message}`
    };
  }
};

module.exports = { jobQueueMQ };
