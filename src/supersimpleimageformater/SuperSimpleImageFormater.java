/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package supersimpleimageformater;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import loci.common.DebugTools;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.FormatException;
import loci.formats.ImageWriter;
import loci.formats.Memoizer;
import loci.formats.meta.MetadataRetrieve;
import loci.formats.meta.MetadataStore;
import loci.formats.ome.OMEXMLMetadataImpl;
import loci.formats.services.OMEXMLService;
import ome.xml.meta.IMetadata;
import ome.xml.model.enums.DimensionOrder;
import ome.xml.model.enums.PixelType;
import ome.xml.model.primitives.PositiveInteger;
import supersimpleimageformater.SuperSimpleImageFormater.file_packing_order;

/**
 *
 * @author ed
 */
public
        class SuperSimpleImageFormater
{

    public
            enum file_packing_order
    {
        ZTC(0),
        ZCT(1),
        TZC(2),
        TCZ(3),
        CZT(4),
        CTZ(5);

        private final
                int intValue;

        file_packing_order(int value)
        {
            this.intValue = value;
        }

        public
                int IntValue()
        {
            return intValue;
        }

        public
                DimensionOrder GetOMEDimensionOrder()
        {
            DimensionOrder order = null;

            switch (this)
            {
                case CTZ:
                {
                    order = DimensionOrder.XYCTZ;
                }
                break;

                case TCZ:
                {
                    order = DimensionOrder.XYTCZ;
                }
                break;

                case CZT:
                {
                    order = DimensionOrder.XYCZT;
                }
                break;

                case TZC:
                {
                    order = DimensionOrder.XYTZC;
                }
                break;

                case ZCT:
                {
                    order = DimensionOrder.XYZCT;
                }
                break;

                case ZTC:
                {
                    order = DimensionOrder.XYZTC;
                }
                break;
            }

            return order;
        }

        public static
                file_packing_order GetPackingOrder(int index)
        {
            file_packing_order result = null;
            for (file_packing_order order : file_packing_order.values())
            {
                if (order.IntValue() == index)
                {
                    result = order;
                    break;
                }
            }
            return result;
        }
    }

    final static
            int N_THREADS = Runtime.getRuntime().availableProcessors();

    public static
            void main(String[] args) throws FileNotFoundException,
                                            IOException, FormatException, DependencyException, ServiceException, DataFormatException, InterruptedException, ExecutionException
    {
        DebugTools.enableLogging("INFO");

        String fileName = args[0];
        if (fileName.endsWith(".ssif"))
        {
            ConvertSSIF(fileName);
        }
        else
        {
            CreateSSIF(fileName);
        }
    }

    public static
            void ConvertSSIF(String inputFile) throws FileNotFoundException,
                                                      IOException, FormatException, DependencyException, ServiceException, DataFormatException, InterruptedException, ExecutionException
    {
        File file = new File(inputFile);
        String fileName = file.getName();
        if (fileName != null && fileName.contains("."))
        {
            fileName = fileName.substring(0, fileName.lastIndexOf('.'));
        }

        if (fileName == null)
        {
            System.err.println("Error: no file name given.");
            return;
        }

        File outputFile = new File(file.getParent(), fileName + ".ome.tiff");

        ServiceFactory factory = new ServiceFactory();
        OMEXMLService service = factory.getInstance(OMEXMLService.class);
        IMetadata omexml = service.createOMEXMLMetadata();

        omexml.setImageID("Image:0", 0);
        omexml.setPixelsID("Pixels:0", 0);

        omexml.setPixelsBinDataBigEndian(Boolean.TRUE, 0, 0);

        try (MultiThreadedReaderAndDeCompressor reader = new MultiThreadedReaderAndDeCompressor(N_THREADS, file))
        {
            omexml.setPixelsDimensionOrder(reader.packingOrder().GetOMEDimensionOrder(), 0);

            switch (reader.bytesPerPixel())
            {
                case 4:
                    omexml.setPixelsType(PixelType.UINT32, 0);
                    break;
                case 2:
                    omexml.setPixelsType(PixelType.UINT16, 0);
                    break;
                case 1:
                    omexml.setPixelsType(PixelType.UINT8, 0);
                    break;
                default:
                    throw new IOException("No encoding scheme for " + reader.bytesPerPixel() + " bytes per pixel");
            }

            omexml.setPixelsSizeX(new PositiveInteger(reader.width()), 0);
            omexml.setPixelsSizeY(new PositiveInteger(reader.height()), 0);
            omexml.setPixelsSizeZ(new PositiveInteger(reader.depth()), 0);
            omexml.setPixelsSizeC(new PositiveInteger(reader.channels()), 0);
            omexml.setPixelsSizeT(new PositiveInteger(reader.timepoints()), 0);

            String[] channelNames = reader.channelNames();
            for (int index = 0;
                 index < reader.channels();
                 ++index)
            {
                omexml.setChannelID("Channel:0:" + index, 0, index);
                omexml.setChannelName(channelNames[index], 0, index);
                omexml.setChannelSamplesPerPixel(new PositiveInteger(1), 0, index);
            }

            try (ImageWriter writer = new ImageWriter())
            {
                writer.setMetadataRetrieve((MetadataRetrieve) omexml);
                writer.setCompression("LZW");
                writer.setWriteSequentially(true);
                writer.setId(outputFile.getCanonicalPath());

                int nImages = reader.depth() * reader.timepoints() * reader.channels();

                int index_2 = 0;
                for (int index = 0;
                     index < nImages;
                     ++index)
                {
                    byte[] result = reader.SubmitFetchAndDeCompressTask();

                    if (result != null)
                    {

                        writer.saveBytes(index_2, result);

                        System.out.print(String.format("\r%1.2f%% complete"
                                                       + "...", 100f
                                                                * (float) (++index_2)
                                                                / (float) nImages));
                        System.out.flush();
                    }
                }

                while (!reader.IsEmpty())
                {

                    byte[] result = reader.GetNextResult();

                    writer.saveBytes(index_2, result);

                    System.out.print(String.format("\r%1.2f%% complete"
                                                   + "...", 100f
                                                            * (float) (++index_2)
                                                            / (float) nImages));
                    System.out.flush();

                }
            }
        }
    }

    public static
            void CreateSSIF(String inputFile) throws FileNotFoundException,
                                                     IOException, FormatException, InterruptedException, ExecutionException
    {
        File file = new File(inputFile);
        String fileName = file.getName();
        if (fileName != null && fileName.contains("."))
        {
            fileName = fileName.substring(0, fileName.lastIndexOf('.'));

            if (fileName.endsWith(".ome"))
            {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
        }

        if (fileName == null)
        {
            System.err.println("Error: no file name given.");
            return;
        }

        try (MultiThreadedReaderAndCompressor imageReader = new MultiThreadedReaderAndCompressor(N_THREADS, file.getCanonicalPath()))
        {
            int nSeries = imageReader.nSeries();
            int width = imageReader.width();
            int height = imageReader.height();
            int depth = imageReader.depth();
            int timepoints = imageReader.timepoints();
            int channels = imageReader.channels();
            int bytesPerPixel = imageReader.bytesPerPixel();
            file_packing_order defaultOrder = file_packing_order.ZTC;
            int nImages = depth * timepoints * channels;

            System.out.println("Splitting into " + nSeries + " files...");

            ByteBuffer intBuff = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            ByteBuffer headerBuffer = ByteBuffer.allocate(92).
                    order(ByteOrder.LITTLE_ENDIAN);
            headerBuffer.putInt(64, width);
            headerBuffer.putInt(68, height);
            headerBuffer.putInt(72, depth);
            headerBuffer.putInt(76, timepoints);
            headerBuffer.putInt(80, channels);
            headerBuffer.putInt(84, bytesPerPixel);
            headerBuffer.putInt(88, defaultOrder.IntValue());

            ByteBuffer channelNamesBuffer = ByteBuffer.allocate(32 * channels).
                    order(ByteOrder.LITTLE_ENDIAN);
            MetadataStore metaStore = imageReader.metadataStore();
            OMEXMLMetadataImpl meta = (OMEXMLMetadataImpl) metaStore;
            for (int chIndex = 0;
                 chIndex < channels;
                 ++chIndex)
            {
                String chName = meta.getChannelName(0, chIndex);
                channelNamesBuffer.position(32 * chIndex);
                channelNamesBuffer.put(chName.getBytes());
            }

            for (int seriesIndex = 0;
                 seriesIndex < nSeries;
                 ++seriesIndex)
            {
                String subFileName = fileName + "_" + seriesIndex;
                File outputFile = new File(file.getParent(), subFileName + ".ssif");

                imageReader.SetSeries(seriesIndex);

                try (FileOutputStream fout = new FileOutputStream(outputFile))
                {
                    headerBuffer.clear();
                    headerBuffer.put(subFileName.getBytes());
                    fout.write(headerBuffer.array());
                    fout.write(channelNamesBuffer.array());

                    int index = 0;
                    for (int iChannel = 0;
                         iChannel < channels;
                         ++iChannel)
                    {
                        for (int iTimepoint = 0;
                             iTimepoint < timepoints;
                             ++iTimepoint)
                        {
                            for (int iDepth = 0;
                                 iDepth < depth;
                                 ++iDepth)
                            {

                                MultiThreadedReaderAndCompressor_Result result = imageReader.SubmitFetchAndCompressTask(iDepth, iTimepoint, iChannel);

                                if (result != null)
                                {
                                    int compressedDataLength = result.length;
                                    byte[] compressionBuffer = result.data;

                                    intBuff.putInt(compressedDataLength);
                                    fout.write(intBuff.array());
                                    intBuff.clear();

                                    fout.write(compressionBuffer, 0, compressedDataLength);

                                    System.out.print(String.format("\r%1.2f%% complete (file %d/%d)"
                                                                   + "...", 100f
                                                                            * (float) (++index)
                                                                            / (float) nImages, seriesIndex + 1, nSeries));
                                    System.out.flush();
                                }
                            }
                        }
                    }

                    while (!imageReader.IsEmpty())
                    {
                        MultiThreadedReaderAndCompressor_Result result = imageReader.GetNextResult();

                        int compressedDataLength = result.length;
                        byte[] compressionBuffer = result.data;

                        intBuff.putInt(compressedDataLength);
                        fout.write(intBuff.array());
                        intBuff.clear();

                        fout.write(compressionBuffer, 0, compressedDataLength);

                        System.out.print(String.format("\r%1.2f%% complete (file %d/%d)"
                                                       + "...", 100f
                                                                * (float) (++index)
                                                                / (float) nImages, seriesIndex + 1, nSeries));
                        System.out.flush();
                    }

                    fout.flush();
                }
            }
        }
    }
}

abstract
        class LittleToBigEndiness
{

    abstract
            void TwiddleBits(byte[] bytes);
}

class ByteConverter_LittleToBig extends LittleToBigEndiness
{

    @Override
    void TwiddleBits(byte[] bytes)
    {
        // don't need to do anything for a byte converter
    }

}

class ShortConverter_LittleToBig extends LittleToBigEndiness
{

    final
            short[] shorts;

    public
            ShortConverter_LittleToBig(int length)
    {
        shorts = new short[length];
    }

    @Override
    void TwiddleBits(byte[] bytes)
    {
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(shorts);
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asShortBuffer().put(shorts);
    }
}

class IntConverter_LittleToBig extends LittleToBigEndiness
{

    final
            int[] ints;

    public
            IntConverter_LittleToBig(int length)
    {
        ints = new int[length];
    }

    @Override
    void TwiddleBits(byte[] bytes)
    {
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(ints);
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asIntBuffer().put(ints);
    }
}

abstract
        class BigToLittleEndiness
{

    abstract
            void TwiddleBits(byte[] bytes);
}

class ByteConverter_BigToLittle extends BigToLittleEndiness
{

    @Override
    void TwiddleBits(byte[] bytes)
    {
        // don't need to do anything for a byte converter
    }

}

class ShortConverter_BigToLittle extends BigToLittleEndiness
{

    final
            short[] shorts;

    public
            ShortConverter_BigToLittle(int length)
    {
        shorts = new short[length];
    }

    @Override
    void TwiddleBits(byte[] bytes)
    {
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asShortBuffer().get(shorts);
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().put(shorts);
    }
}

class IntConverter_BigToLittle extends BigToLittleEndiness
{

    final
            int[] ints;

    public
            IntConverter_BigToLittle(int length)
    {
        ints = new int[length];
    }

    @Override
    void TwiddleBits(byte[] bytes)
    {
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asIntBuffer().get(ints);
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(ints);
    }
}

class MultiThreadedReaderAndCompressor_Result
{

    byte[] data;
    int length;

    public
            MultiThreadedReaderAndCompressor_Result(byte[] data, int length)
    {
        this.data = data;
        this.length = length;
    }
}

class MultiThreadedReaderAndCompressor implements AutoCloseable
{

    class ThreadContext implements AutoCloseable
    {

        final
                Memoizer reader;
        final
                Deflater compressor;
        final
                BigToLittleEndiness converter;
        final
                byte[][] compressionBuffer;
        final
                AtomicInteger pingpong;

        public
                ThreadContext(String id) throws FormatException, IOException
        {
            reader = new Memoizer();
            reader.setId(id);

            int nPixelsPerPlane = reader.getSizeX() * reader.getSizeY();
            int bytesPerPixel = reader.getBitsPerPixel() / 8;

            if (reader.isLittleEndian())
            {
                converter = null;
            }
            else
            {
                switch (bytesPerPixel)
                {
                    case 4:
                        converter = new IntConverter_BigToLittle(nPixelsPerPlane);
                        break;
                    case 2:
                        converter = new ShortConverter_BigToLittle(nPixelsPerPlane);
                        break;
                    case 1:
                        converter = new ByteConverter_BigToLittle();
                        break;
                    default:
                        throw new IOException("No encoding scheme for " + bytesPerPixel + " bytes per pixel");
                }
            }

            compressor = new Deflater(Deflater.BEST_COMPRESSION);
            compressionBuffer = new byte[2][2 * bytesPerPixel * nPixelsPerPlane];
            pingpong = new AtomicInteger(0);
        }

        byte[] GetNextCompressionBuffer()
        {
            int index = pingpong.get();
            byte[] result = compressionBuffer[index];
            pingpong.set((index + 1) % 2);

            return result;
        }

        int nSeries()
        {
            return reader.getSeriesCount();
        }

        int width()
        {
            return reader.getSizeX();
        }

        int height()
        {
            return reader.getSizeY();
        }

        int depth()
        {
            return reader.getSizeZ();
        }

        int timepoints()
        {
            return reader.getSizeT();
        }

        int channels()
        {
            return reader.getSizeC();
        }

        int bytesPerPixel()
        {
            return reader.getBitsPerPixel() / 8;
        }

        MetadataStore metadataStore()
        {
            return reader.getMetadataStore();
        }

        void setSeries(int index)
        {
            reader.setSeries(index);
        }

        @Override
        public
                void close() throws IOException
        {
            reader.close();
        }
    }

    final
            ThreadContext[] contexts;
    final
            ExecutorService executor;
    final
            AtomicInteger currentThread;
    final
            AtomicInteger nRunningTasks;
    final
            ArrayList<Future<MultiThreadedReaderAndCompressor_Result>> results;

    public
            MultiThreadedReaderAndCompressor(int nThreads, String inputFile) throws InterruptedException, ExecutionException
    {
        contexts = new ThreadContext[nThreads];
        executor = Executors.newFixedThreadPool(nThreads);
        currentThread = new AtomicInteger(0);
        nRunningTasks = new AtomicInteger(0);
        results = new ArrayList<>(nThreads);

        ArrayList<Future<?>> tasks = new ArrayList<>(nThreads);
        for (int index = 0;
             index < nThreads;
             ++index)
        {
            final
                    int myId = index;
            results.add(null);
            tasks.add(executor.submit(() ->
            {
                try
                {
                    contexts[myId] = new ThreadContext(inputFile);
                }
                catch (FormatException | IOException ex)
                {
                    Logger.getLogger(MultiThreadedReaderAndCompressor.class.getName()).log(Level.SEVERE, null, ex);
                }
            }));
        }

        for (Future<?> task : tasks)
        {
            task.get();
        }
    }

    public
            void SetSeries(int seriesIndex) throws InterruptedException, ExecutionException
    {
        ArrayList<Future<?>> tasks = new ArrayList<>(contexts.length);
        for (int index = 0;
             index < contexts.length;
             ++index)
        {
            final
                    int myId = index;
            tasks.add(executor.submit(() ->
            {
                contexts[myId].setSeries(seriesIndex);
            }));
        }

        for (Future<?> task : tasks)
        {
            task.get();
        }
    }

    public
            boolean IsFull()
    {
        return nRunningTasks.get() == contexts.length;
    }

    public
            boolean IsEmpty()
    {
        return nRunningTasks.get() == 0;
    }

    public
            int nSeries()
    {
        return contexts[0].nSeries();
    }

    public
            int width()
    {
        return contexts[0].width();
    }

    public
            int height()
    {
        return contexts[0].height();
    }

    public
            int depth()
    {
        return contexts[0].depth();
    }

    public
            int timepoints()
    {
        return contexts[0].timepoints();
    }

    public
            MetadataStore metadataStore()
    {
        return contexts[0].metadataStore();
    }

    int channels()
    {
        return contexts[0].channels();
    }

    int bytesPerPixel()
    {
        return contexts[0].bytesPerPixel();
    }

    MultiThreadedReaderAndCompressor_Result
            GetNextResult() throws InterruptedException, ExecutionException
    {
        int myId = currentThread.get();

        if (currentThread.incrementAndGet() == contexts.length)
        {
            currentThread.set(0);
        }

        MultiThreadedReaderAndCompressor_Result functionResult = results.get(myId).get();
        nRunningTasks.decrementAndGet();

        return functionResult;
    }

    MultiThreadedReaderAndCompressor_Result
            SubmitFetchAndCompressTask(int z, int t, int c) throws InterruptedException, ExecutionException
    {
        final
                int myId;
        MultiThreadedReaderAndCompressor_Result functionResult;

        if (!IsFull())
        {
            myId = nRunningTasks.get();
            nRunningTasks.incrementAndGet();

            functionResult = null;
        }
        else
        {
            myId = currentThread.get();

            if (currentThread.incrementAndGet() == contexts.length)
            {
                currentThread.set(0);
            }

            functionResult = results.get(myId).get();
        }

        results.set(myId, executor.submit((Callable<MultiThreadedReaderAndCompressor_Result>) () ->
            {
                Deflater compressor = contexts[myId].compressor;
                Memoizer reader = contexts[myId].reader;
                BigToLittleEndiness converter = contexts[myId].converter;
                byte[] compressionBuffer = contexts[myId].GetNextCompressionBuffer();
                MultiThreadedReaderAndCompressor_Result result;

                if (converter == null)
                {
                    try
                    {
                        compressor.setInput(reader.openBytes(reader.
                                getIndex(z, c, t)));
                    }
                    catch (FormatException | IOException ex)
                    {
                        Logger.getLogger(MultiThreadedReaderAndCompressor.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                else
                {
                    byte[] bytes = null;
                    try
                    {
                        bytes = reader.openBytes(reader.
                                getIndex(z, c, t));
                    }
                    catch (FormatException | IOException ex)
                    {
                        Logger.getLogger(MultiThreadedReaderAndCompressor.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    converter.TwiddleBits(bytes);
                    compressor.setInput(bytes);
                }

                compressor.finish();
                int compressedDataLength = compressor.deflate(compressionBuffer);
                compressor.reset();

                result = new MultiThreadedReaderAndCompressor_Result(compressionBuffer, compressedDataLength);

                return (result);
            }));

        return functionResult;
    }

    @Override
    public
            void close() throws InterruptedException, ExecutionException
    {
        ArrayList<Future<?>> tasks = new ArrayList<>(contexts.length);
        for (int index = 0;
             index < contexts.length;
             ++index)
        {
            final
                    int myId = index;
            tasks.add(executor.submit(() ->
            {
                try
                {
                    contexts[myId].close();
                }
                catch (IOException ex)
                {
                    Logger.getLogger(MultiThreadedReaderAndCompressor.class.getName()).log(Level.SEVERE, null, ex);
                }
            }));
        }

        for (Future<?> task : tasks)
        {
            task.get();
        }

        executor.shutdown();
    }
}

class MultiThreadedReaderAndDeCompressor implements AutoCloseable
{

    class ThreadContext implements AutoCloseable
    {

        final
                FileInputStream reader;
        final
                Inflater decompressor;
        final
                LittleToBigEndiness converter;
        final
                byte[][] decompressionBuffer;
        final
                byte[] compressedImageBuffer;
        final
                AtomicInteger pingpong;
        final
                ByteBuffer intBuffer;
        final
                int width;
        final
                int height;
        final
                int depth;
        final
                int timepoints;
        final
                int channels;
        final
                int nBytesPerPixel;
        final
                file_packing_order packingOrder;
        final
                String[] channelNames;

        public
                ThreadContext(File file) throws FormatException, IOException
        {
            reader = new FileInputStream(file);

            ByteBuffer headerBuffer = ByteBuffer.allocate(92).
                    order(ByteOrder.LITTLE_ENDIAN);
            reader.read(headerBuffer.array());

            //byte[] nameBytes = new byte[64];
            //headerBuffer.get(nameBytes);
            headerBuffer.position(64);

            width = headerBuffer.getInt();
            height = headerBuffer.getInt();
            depth = headerBuffer.getInt();
            timepoints = headerBuffer.getInt();
            channels = headerBuffer.getInt();
            nBytesPerPixel = headerBuffer.getInt();
            packingOrder = file_packing_order.GetPackingOrder(headerBuffer.getInt());

            ByteBuffer chNamesBuffer = ByteBuffer.allocate(32 * channels).
                    order(ByteOrder.LITTLE_ENDIAN);
            reader.read(chNamesBuffer.array());

            channelNames = new String[channels];
            for (int chIndex = 0;
                 chIndex < channels;
                 ++chIndex)
            {
                byte[] namebytes = new byte[32];
                chNamesBuffer.position(32 * chIndex);
                chNamesBuffer.get(namebytes);
                channelNames[chIndex] = new String(namebytes).trim();
            }

            switch (nBytesPerPixel)
            {
                case 4:
                    converter = new IntConverter_LittleToBig(width * height);
                    break;
                case 2:
                    converter = new ShortConverter_LittleToBig(width * height);
                    break;
                case 1:
                    converter = new ByteConverter_LittleToBig();
                    break;
                default:
                    throw new IOException("No encoding scheme for " + nBytesPerPixel + " bytes per pixel");
            }

            intBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            decompressor = new Inflater();
            decompressionBuffer = new byte[2][nBytesPerPixel * width * height];
            compressedImageBuffer = new byte[2 * nBytesPerPixel * width * height];
            pingpong = new AtomicInteger(0);
        }

        byte[] GetNextDeCompressionBuffer()
        {
            int index = pingpong.get();
            byte[] result = decompressionBuffer[index];
            pingpong.set((index + 1) % 2);

            return result;
        }

        int width()
        {
            return width;
        }

        int height()
        {
            return height;
        }

        int depth()
        {
            return depth;
        }

        int timepoints()
        {
            return timepoints;
        }

        int channels()
        {
            return channels;
        }

        int bytesPerPixel()
        {
            return nBytesPerPixel;
        }

        file_packing_order packingOrder()
        {
            return packingOrder;
        }

        String[] channelNames()
        {
            return channelNames;
        }

        @Override
        public
                void close() throws IOException
        {
            reader.close();
        }
    }

    final
            ThreadContext[] contexts;
    final
            ExecutorService executor;
    final
            AtomicInteger currentThread;
    final
            AtomicInteger nRunningTasks;
    final
            ArrayList<Future<byte[]>> results;
    final
            int nThreads;

    public
            MultiThreadedReaderAndDeCompressor(int nThreads, File inputFile) throws InterruptedException, ExecutionException
    {
        this.nThreads = nThreads;
        contexts = new ThreadContext[nThreads];
        executor = Executors.newFixedThreadPool(nThreads);
        currentThread = new AtomicInteger(0);
        nRunningTasks = new AtomicInteger(0);
        results = new ArrayList<>(nThreads);

        ArrayList<Future<?>> tasks = new ArrayList<>(nThreads);
        for (int index = 0;
             index < nThreads;
             ++index)
        {
            final
                    int myId = index;
            results.add(null);
            tasks.add(executor.submit(() ->
            {
                try
                {
                    contexts[myId] = new ThreadContext(inputFile);

                    ByteBuffer intBuffer = contexts[myId].intBuffer;
                    FileInputStream reader = contexts[myId].reader;

                    for (int index2 = 0;
                         index2 < myId;
                         ++index2)
                    {
                        reader.read(intBuffer.array());
                        int nBytes = intBuffer.getInt();
                        intBuffer.clear();
                        reader.skip(nBytes);
                    }
                }
                catch (FormatException | IOException ex)
                {
                    Logger.getLogger(MultiThreadedReaderAndCompressor.class.getName()).log(Level.SEVERE, null, ex);
                }
            }));
        }

        for (Future<?> task : tasks)
        {
            task.get();
        }
    }

    public
            boolean IsFull()
    {
        return nRunningTasks.get() == nThreads;
    }

    public
            boolean IsEmpty()
    {
        return nRunningTasks.get() == 0;
    }

    public
            int width()
    {
        return contexts[0].width();
    }

    public
            int height()
    {
        return contexts[0].height();
    }

    public
            int depth()
    {
        return contexts[0].depth();
    }

    public
            int timepoints()
    {
        return contexts[0].timepoints();
    }

    int channels()
    {
        return contexts[0].channels();
    }

    int bytesPerPixel()
    {
        return contexts[0].bytesPerPixel();
    }

    file_packing_order packingOrder()
    {
        return contexts[0].packingOrder();
    }

    public
            String[] channelNames()
    {
        return contexts[0].channelNames();
    }

    byte[]
            GetNextResult() throws InterruptedException, ExecutionException
    {
        int myId = currentThread.get();

        if (currentThread.incrementAndGet() == nThreads)
        {
            currentThread.set(0);
        }

        byte[] functionResult = results.get(myId).get();
        nRunningTasks.decrementAndGet();

        return functionResult;
    }

    byte[] SubmitFetchAndDeCompressTask() throws InterruptedException, ExecutionException
    {
        final
                int myId;
        byte[] functionResult;

        if (!IsFull())
        {
            myId = nRunningTasks.get();
            nRunningTasks.incrementAndGet();

            functionResult = null;
        }
        else
        {
            myId = currentThread.get();

            if (currentThread.incrementAndGet() == nThreads)
            {
                currentThread.set(0);
            }

            functionResult = results.get(myId).get();
        }

        results.set(myId, executor.submit((Callable<byte[]>) () ->
            {
                Inflater decompressor = contexts[myId].decompressor;
                FileInputStream reader = contexts[myId].reader;
                LittleToBigEndiness converter = contexts[myId].converter;
                byte[] result = contexts[myId].GetNextDeCompressionBuffer();
                byte[] compressedImageBuffer = contexts[myId].compressedImageBuffer;
                ByteBuffer intBuffer = contexts[myId].intBuffer;

                reader.read(intBuffer.array());
                int nBytes = intBuffer.getInt();
                intBuffer.clear();
                reader.read(compressedImageBuffer, 0, nBytes);

                decompressor.setInput(compressedImageBuffer, 0, nBytes);
                decompressor.inflate(result);
                decompressor.reset();

                converter.TwiddleBits(result);

                for (int index = 0;
                     index < (nThreads - 1);
                     ++index)
                {
                    reader.read(intBuffer.array());
                    nBytes = intBuffer.getInt();
                    intBuffer.clear();
                    reader.skip(nBytes);
                }

                return (result);
            }));

        return functionResult;
    }

    @Override
    public
            void close() throws InterruptedException, ExecutionException
    {
        ArrayList<Future<?>> tasks = new ArrayList<>(contexts.length);
        for (int index = 0;
             index < contexts.length;
             ++index)
        {
            final
                    int myId = index;
            tasks.add(executor.submit(() ->
            {
                try
                {
                    contexts[myId].close();
                }
                catch (IOException ex)
                {
                    Logger.getLogger(MultiThreadedReaderAndCompressor.class.getName()).log(Level.SEVERE, null, ex);
                }
            }));
        }

        for (Future<?> task : tasks)
        {
            task.get();
        }

        executor.shutdown();
    }
}
