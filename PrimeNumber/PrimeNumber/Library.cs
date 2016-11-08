using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace PrimeNumber
{
    public static class Library
    {

        private static int MAX_SEGMENT_SIZE = 15000;
        private const int MAX_SEGMENTS = 7;

        public static List<long> GetPrimeSoE(long limit, int segmentSize = 0)
        {
            if (segmentSize != 0)
            {
                MAX_SEGMENT_SIZE = segmentSize;
            }
            
            var timer = DateTime.Now;
            var primes = new List<long>(){2};

            if (limit < 2)
            {
                return new List<long>();
            }
            else if (limit == 2)
            {
                return primes;
            }

            int segNum = 0;
            int segSize = 0;

            
            Queue<SoESieveSegment> sieveSegs = new Queue<SoESieveSegment>();
            // Begin creating sieve segments and queueing them up for processing
            // Will only segment up to the max number of segments specified by MAX_SEGMENTS to limit
            // the number of threads that can be processing at one time
            for (int i = 0; i <= MAX_SEGMENTS; i++)
            {
                segNum++;
                segSize = GetSieveSegmentSize(segNum, limit);
                sieveSegs.Enqueue(new SoESieveSegment((segNum-1)*MAX_SEGMENT_SIZE, segSize, new ConcurrentQueue<long>()));
                
                // If you've segmented out sieves up to the limit, then you're done, and don't
                // need to create anymore sieves
                if ((segNum-1)*MAX_SEGMENT_SIZE + segSize == limit)
                {
                    break;
                }
            }
            //TODO: Consolidate the use of this equation in one place if possible
            var limitReached = (segNum - 1) * MAX_SEGMENT_SIZE + segSize == limit;
            // Grab the first sieve segment seg to be our lead segment
            SoESieveSegment leadSieveSeg = sieveSegs.Dequeue();
            leadSieveSeg.isLead = true;
            // The First prime number to process is always 3 SoESieveSegment will always skip over even numbers
            long primeToProcess = 3;

            // While there are still sieve segments and prime numbers to process
            while (sieveSegs.Count > 0 || primeToProcess > 0) 
            {
                // If the prime number that we're processing is less than 0, then we've reached the end of the sieve
                // segment, and need to move on to the next one in the queue
                if (primeToProcess < 0)
                {
                    // Store off all of the prime numbers in the current sieve segment
                    primes.AddRange(leadSieveSeg.GetPrimes());
                    // Grab next sieve segment out of the queue to be our lead segment

                    if (sieveSegs.Peek().IsProcessing)
                        sieveSegs.Peek().Task.Wait();
                    
                    leadSieveSeg = sieveSegs.Dequeue();
                    leadSieveSeg.isLead = true;
                    // Grab the next available prime number from the new sieve segment
                    primeToProcess = leadSieveSeg.GetNextPrime();

                    // If we've not yet reached the limit, queue up a new sieve segment, and begin processing
                    // the existing prime numbers
                    if (!limitReached)
                    {
                        segNum++;

                        segSize = GetSieveSegmentSize(segNum, limit);
                        limitReached = (segNum - 1) * MAX_SEGMENT_SIZE + segSize == limit;
                        sieveSegs.Enqueue(new SoESieveSegment((segNum - 1) * MAX_SEGMENT_SIZE, segSize, new ConcurrentQueue<long>(primes)));
                    }
                }
                else
                {
                    // Loop through the sieve segs still in our queue, and enqueue the current 
                    // prime number to be processed
                    foreach (SoESieveSegment s in sieveSegs)
                    {
                        s.EnqueuePrime(primeToProcess);
                    }

                    // Enqueue the current prime number to process and process that one for
                    // the lead sieve segment
                    leadSieveSeg.EnqueuePrime(primeToProcess);

                    //Once finished processing the current prime number, get the next one after it
                    primeToProcess = leadSieveSeg.GetNextPrime(primeToProcess);
                }
            }
            primes.AddRange(leadSieveSeg.GetPrimes());
            return primes;
        }

        //TODO: Fix this to work where the segment size is a number greater than the limit
        private static int GetSieveSegmentSize(int segNum, long limit)
        {
            var segSize = 0;
            if (segNum * MAX_SEGMENT_SIZE > limit)
            {
                segSize = (int)(limit - ((segNum-1) * MAX_SEGMENT_SIZE));
            }
            else
            {
                segSize = MAX_SEGMENT_SIZE;
            }

            return segSize;
        }

    }

    public class SoESieveSegment
    {
        public bool[] SieveSeg { get; set; }
        public long Offset { get; set; }
        public bool isLead;
        public Task Task;

        private bool _isProcessing;
        public bool IsProcessing
        {
            get
            {
                return _isProcessing;
            }
        }

        private bool disposed = false;
        // Instantiate a SafeHandle instance.
        private SafeHandle handle = new SafeFileHandle(IntPtr.Zero, true);

        private ConcurrentQueue<long> primeQueue { get; set; }

        public SoESieveSegment(long offset, int sieveSize, ConcurrentQueue<long> queue)
        {
            SieveSeg = new bool[sieveSize];
            Offset = offset;
            if (Offset == 0)
            {
                //Assumes that we'll always start the sieve at 3
                SieveSeg[0] = true;
                SieveSeg[1] = true;
                SieveSeg[2] = true;
            }

            primeQueue = queue;
            _isProcessing = false;
            isLead = false;

            if (!primeQueue.IsEmpty)
            {
                Task = Task.Factory.StartNew( () => ProcessQueue());
            }
        }

        public void ProcessQueue()
        {
            //Possibly pull existing queue into thread (clear queue after making copy but before entering thread), process, then check for new queue of things to pull in and repeat
            //Hopefully keep from having to make enter/exit or recreate a bunch of threads repeatedly
            _isProcessing = true;
            long prime = 0;
            long startIndex = 0;
            long primeSqr = 0;
            bool dequeueSuccessful = false;

            while (primeQueue.Count > 0)
            {
                dequeueSuccessful = primeQueue.TryDequeue(out prime);
                if (dequeueSuccessful)
                {
                    primeSqr = prime * prime;

                    if (primeSqr < SieveSeg.Length + Offset)
                    {
                        if (primeSqr < Offset)
                        {
                            for (int i = 1; i < SieveSeg.Length; i += 2)
                            {
                                if ((i + Offset) % prime == 0)
                                {
                                    startIndex = i;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            startIndex = primeSqr - Offset;
                        }

                        for (long i = startIndex; i < SieveSeg.Length; i += prime)
                        {
                            SieveSeg[i] = true;
                        }
                    }
                    dequeueSuccessful = false;
                }
            }
            _isProcessing = false;
        }

        public long GetNextPrime(long currentPrime = 0)
        {
            // Initialize the startIndex to the currentPrime
            var startIndex = currentPrime;

            // Increment the current prime number to the next odd number
            if (startIndex % 2 == 0)
                startIndex += 1;
            else
                startIndex += 2;

            // If the startIndex is greater than the Sieve Segment size,
            // subtract the offset from it
            if (startIndex >= Offset)
                startIndex -= Offset;

            long prime = -1;
            for (long i = startIndex; i < SieveSeg.Length; i += 2)
            {
                if (!SieveSeg[i])
                {
                    prime = (i + Offset);
                    break;
                }
            }
            return prime;
        }

        public List<long> GetPrimes()
        {
            var primes = new List<long>();

            for (int i = 1; i < SieveSeg.Length; i+=2)
            {
                if (!SieveSeg[i])
                {
                    primes.Add(i + Offset);
                }
            }
            return primes;
        }

        public void EnqueuePrime(long p)
        {
            primeQueue.Enqueue(p);

            if (!_isProcessing)
            {
                if (isLead)
                {
                    ProcessQueue();
                }
                else
                {
                    Task = Task.Factory.StartNew(() => ProcessQueue());
                }                
            }
        }
    }
}
