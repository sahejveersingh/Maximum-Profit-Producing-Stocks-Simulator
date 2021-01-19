int maxProfit(int* prices, int pricesSize){
        int i = 0, b, s, profit = 0;
        for(;i<pricesSize - 1;) {
           for(;i < pricesSize - 1 && prices[i + 1] <= prices[i];) i++;
            b = prices[i];

            for (;i < pricesSize - 1 && prices[i + 1] > prices[i];) i++;
            s = prices[i];

            profit = profit + s - b;
        }
        return profit;
}

